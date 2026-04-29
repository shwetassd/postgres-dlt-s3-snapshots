import os
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv

from src.config_loader.loader import (
    load_database_config,
    load_delta_load_tables,
    load_full_load_tables,
    load_settings,
)
from src.pipelines.delta_load_pipeline import run_delta_snapshot
from src.pipelines.full_load_pipeline import run_full_snapshot
from src.sources.postgres import get_engine
from src.utils.runtime_config import get_bucket_url, get_snapshot_date
from src.utils.s3_cleanup import delete_table_snapshot_prefix
from src.utils.pipeline_names import build_pipeline_name
from src.utils.dlt_runtime import get_pipelines_dir
from src.utils.load_metrics import total_rows
from src.utils.logging_config import (
    configure_logging,
    flush_logging_handlers,
    refresh_dlt_log_levels,
    silence_common_warnings,
)
from src.utils.dlt_project import ensure_dlt_config_from_repo
from src.utils.sns_notify import publish_load_failures
from src.utils.workdir_cleanup import clear_pipeline_workdir

load_dotenv()
silence_common_warnings()
log = configure_logging()
ensure_dlt_config_from_repo()


def log_start(load_type: str, db: str, schema: str, table: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    log.info("[%s] [START %s] %s.%s.%s", now, load_type, db, schema, table)


def log_end(
    load_type: str,
    db: str,
    schema: str,
    table: str,
    start_time: datetime,
    *,
    success: bool,
    error: Exception | None = None,
    norm_counts: dict[str, int] | None = None,
) -> None:
    duration = int((datetime.now() - start_time).total_seconds())
    status = "SUCCESS" if success else "FAILED"
    parts = [
        f"[END {load_type}]",
        f"{db}.{schema}.{table}",
        status,
        f"{duration}s",
    ]
    if success and norm_counts is not None:
        parts.append(f"rows_loaded={total_rows(norm_counts)}")
        if len(norm_counts) > 1:
            parts.append(f"per_resource={norm_counts}")
    elif success and norm_counts is None:
        parts.append("rows_loaded=n/a")
    if error is not None:
        parts.append(f"error={error}")
    log.info(" | ".join(parts))


def resolve_database_name(configured_databases: dict[str, str], database_alias: str) -> str:
    env_key = configured_databases.get(database_alias)
    if not env_key:
        raise ValueError(f"No env_database_key configured for database '{database_alias}'")
    actual_db = os.getenv(env_key)
    if not actual_db:
        raise ValueError(f"Environment variable '{env_key}' is not set")
    return actual_db


def get_engine_cached(
    engine_cache: dict[str, object],
    engine_cache_lock: threading.Lock,
    actual_db: str,
):
    with engine_cache_lock:
        engine = engine_cache.get(actual_db)
        if engine is None:
            engine = get_engine(actual_db)
            engine_cache[actual_db] = engine
        return engine


def get_output_table_name(table_cfg) -> str:
    return getattr(table_cfg, "output_table_name", None) or table_cfg.table


def _group_full_tables_by_database(full_tables: list) -> list[tuple[str, list]]:
    """Stable order: sorted database alias (YAML stem) — each group is tables for one alias."""
    groups: dict[str, list] = {}
    for t in full_tables:
        groups.setdefault(t.database, []).append(t)
    return [(alias, groups[alias]) for alias in sorted(groups.keys())]


def _dispose_engine_for_database_alias(
    database_alias: str,
    configured_databases: dict[str, str],
    engine_cache: dict[str, object],
    engine_cache_lock: threading.Lock,
) -> None:
    """Close pooled connections for one logical DB so serial FULL batches do not hold idle pools."""
    try:
        actual_db = resolve_database_name(configured_databases, database_alias)
    except ValueError:
        return
    with engine_cache_lock:
        eng = engine_cache.pop(actual_db, None)
    if eng is not None:
        eng.dispose()


def run_one_full_table(table_cfg, configured_databases, runtime_cfg, engine_cache, engine_cache_lock):
    start_time = datetime.now()
    output_table_name = get_output_table_name(table_cfg)

    log_start("FULL", table_cfg.database, table_cfg.schema, output_table_name)

    actual_db = resolve_database_name(configured_databases, table_cfg.database)
    engine = get_engine_cached(engine_cache, engine_cache_lock, actual_db)

    if runtime_cfg["delete_existing_full_load_snapshot"]:
        if os.getenv("SKIP_DELETE_EXISTING_SNAPSHOT", "").lower() in ("1", "true", "yes"):
            log.info(
                "SKIP_DELETE_EXISTING_SNAPSHOT is set — not deleting existing snapshot prefix",
            )
        else:
            prefix_key = (
                f"{runtime_cfg['dataset_name']}|{runtime_cfg['snapshot_date']}|{output_table_name}"
            )
            cleared = runtime_cfg["full_load_cleared_prefix_keys"]
            lock = runtime_cfg["full_load_delete_lock"]
            with lock:
                if prefix_key in cleared:
                    log.info(
                        "Skip duplicate S3 snapshot delete for output_table=%s "
                        "(already replaced this prefix once in this job — avoids wiping files mid-run)",
                        output_table_name,
                    )
                else:
                    log.info(
                        "Replacing prior snapshot under this prefix for output_table=%s "
                        "(delete runs once before extract; next job run will delete again for same date)",
                        output_table_name,
                    )
                    delete_table_snapshot_prefix(
                        bucket_url=runtime_cfg["bucket_url"],
                        dataset_name=runtime_cfg["dataset_name"],
                        layout_prefix_template=runtime_cfg["full_load_delete_prefix_template"],
                        table_name=output_table_name,
                        snapshot_date=runtime_cfg["snapshot_date"],
                    )
                    cleared.add(prefix_key)

    load_info, norm_counts = run_full_snapshot(
        engine=engine,
        pipeline_name=build_pipeline_name(
            runtime_cfg["pipeline_name"], "full", table_cfg.database, table_cfg.schema, output_table_name
        ),
        pipelines_dir=runtime_cfg["pipelines_dir"],
        dataset_name=runtime_cfg["dataset_name"],
        bucket_url=runtime_cfg["bucket_url"],
        layout=runtime_cfg["full_load_layout"],
        file_format=runtime_cfg["file_format"],
        schema_name=table_cfg.schema,
        source_table_name=table_cfg.table,
        table_name=output_table_name,
        columns=table_cfg.columns,
        select_sql=table_cfg.select_sql,
        output_columns=table_cfg.output_columns,
        snapshot_date=runtime_cfg["snapshot_date"],
        extract_chunk_size=(
            table_cfg.extract_chunk_size
            if table_cfg.extract_chunk_size is not None
            else runtime_cfg["extract_chunk_size"]
        ),
        extract_backend=runtime_cfg["extract_backend"],
    )

    return (
        "FULL",
        table_cfg.database,
        table_cfg.schema,
        output_table_name,
        start_time,
        load_info,
        norm_counts,
    )


def run_one_delta_table(table_cfg, configured_databases, runtime_cfg, engine_cache, engine_cache_lock):
    start_time = datetime.now()
    output_table_name = get_output_table_name(table_cfg)

    log_start("DELTA", table_cfg.database, table_cfg.schema, output_table_name)

    actual_db = resolve_database_name(configured_databases, table_cfg.database)
    engine = get_engine_cached(engine_cache, engine_cache_lock, actual_db)

    load_info, norm_counts = run_delta_snapshot(
        engine=engine,
        pipeline_name=build_pipeline_name(
            runtime_cfg["pipeline_name"], "delta", table_cfg.database, table_cfg.schema, output_table_name
        ),
        pipelines_dir=runtime_cfg["pipelines_dir"],
        dataset_name=runtime_cfg["dataset_name"],
        bucket_url=runtime_cfg["bucket_url"],
        layout=runtime_cfg["delta_load_layout"],
        file_format=runtime_cfg["file_format"],
        schema_name=table_cfg.schema,
        source_table_name=table_cfg.table,
        table_name=output_table_name,
        columns=table_cfg.columns,
        select_sql=table_cfg.select_sql,
        output_columns=table_cfg.output_columns,
        primary_key=table_cfg.primary_key,
        cursor_column=table_cfg.cursor_column,
        initial_value=table_cfg.initial_value,
        write_disposition=table_cfg.write_disposition,
        snapshot_date=runtime_cfg["snapshot_date"],
        extract_chunk_size=runtime_cfg["extract_chunk_size"],
        extract_backend=runtime_cfg["extract_backend"],
    )

    return (
        "DELTA",
        table_cfg.database,
        table_cfg.schema,
        output_table_name,
        start_time,
        load_info,
        norm_counts,
    )


def _phase_watchdog(phase_name: str, future_to_cfg: dict, stop: threading.Event) -> None:
    """Ping logs while table tasks run — lists which tables are still in flight."""
    interval = int(os.getenv("PHASE_WATCHDOG_INTERVAL_SEC", "180"))
    if interval <= 0:
        return
    total = len(future_to_cfg)
    while not stop.wait(interval):
        pending = [f for f in future_to_cfg if not f.done()]
        if not pending:
            continue
        labels = []
        for f in pending:
            cfg = future_to_cfg[f]
            out_name = get_output_table_name(cfg)
            labels.append(f"{cfg.database}.{cfg.schema}.{out_name}")
        log.warning(
            "[%s] Watchdog: %s/%s task(s) still running (not failed yet — list order arbitrary): %s",
            phase_name,
            len(pending),
            total,
            "; ".join(sorted(labels)),
        )


def run_phase(
    phase_name: str,
    tables,
    *,
    max_workers: int,
    worker_fn,
    configured_databases,
    runtime_cfg,
    engine_cache,
    engine_cache_lock,
    debug_load_info: bool,
):
    refresh_dlt_log_levels()
    success = 0
    failed = 0
    failures: list[str] = []
    rows_total = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_cfg = {
            executor.submit(
                worker_fn, t, configured_databases, runtime_cfg, engine_cache, engine_cache_lock
            ): t
            for t in tables
        }
        log.info(
            "%s phase: queued %s table task(s), max_workers=%s",
            phase_name,
            len(tables),
            max_workers,
        )
        if phase_name == "FULL":
            log.info(
                "FULL: If this phase never logs \"phase finished\" below, a worker is still "
                "blocked (long SQL / I/O) or the process was OOM-killed — see PG_STATEMENT_TIMEOUT_MS "
                "and CloudWatch/ECS stop reason.",
            )
        stop_watchdog = threading.Event()
        watchdog = threading.Thread(
            target=_phase_watchdog,
            args=(phase_name, future_to_cfg, stop_watchdog),
            daemon=True,
            name=f"{phase_name}-watchdog",
        )
        watchdog.start()
        try:
            for future in as_completed(future_to_cfg):
                cfg = future_to_cfg[future]
                out_name = get_output_table_name(cfg)
                try:
                    load_type, db, schema, table, start_time, load_info, norm_counts = future.result()
                    rows_total += total_rows(norm_counts)
                    log_end(
                        load_type,
                        db,
                        schema,
                        table,
                        start_time,
                        success=True,
                        norm_counts=norm_counts,
                    )
                    if debug_load_info:
                        log.info("load_info=%s", load_info)
                    if not norm_counts:
                        log.debug(
                            "No normalize row_counts in trace for %s.%s.%s (dlt may omit in some paths)",
                            db,
                            schema,
                            table,
                        )
                    success += 1
                except Exception as e:
                    log.exception(
                        "%s failed database=%s schema=%s source_table=%s output_table=%s",
                        phase_name,
                        cfg.database,
                        cfg.schema,
                        cfg.table,
                        out_name,
                    )
                    failed += 1
                    failures.append(
                        f"{cfg.database}.{cfg.schema}.{out_name}: {type(e).__name__}: {e}"
                    )
        finally:
            stop_watchdog.set()

    log.info("%s phase: all worker threads finished — summary counts follow.", phase_name)
    log.info(
        "%s phase finished: success=%s failed=%s rows_total=%s",
        phase_name,
        success,
        failed,
        rows_total,
    )
    if failed:
        log.error(
            "%s phase: %s table load(s) FAILED — traceback is logged above per failed table. "
            "Process exits with code 1 after RUN SUMMARY if this run has any failed tables.",
            phase_name,
            failed,
        )

    return success, failed, failures, rows_total


def main() -> None:
    overall_start = datetime.now()

    settings = load_settings()
    databases_config = load_database_config()

    full_tables = load_full_load_tables()
    delta_tables = load_delta_load_tables(settings["delta"]["default_write_disposition"])

    run_database = os.getenv("RUN_DATABASE")
    run_table = os.getenv("RUN_TABLE")
    run_load_type = os.getenv("RUN_LOAD_TYPE")

    if run_database:
        full_tables = [t for t in full_tables if t.database == run_database]
        delta_tables = [t for t in delta_tables if t.database == run_database]

    if run_table:
        full_tables = [t for t in full_tables if t.table == run_table]
        delta_tables = [t for t in delta_tables if t.table == run_table]

    if run_load_type:
        normalized = run_load_type.lower().strip()
        if normalized == "full":
            delta_tables = []
        elif normalized == "delta":
            full_tables = []

    log.info(
        "Filters RUN_DATABASE=%s RUN_TABLE=%s RUN_LOAD_TYPE=%s",
        run_database,
        run_table,
        run_load_type,
    )
    log.info(
        "Selected FULL=%s table(s) DELTA=%s table(s)",
        len(full_tables),
        len(delta_tables),
    )

    if (run_database or run_table or run_load_type) and (len(full_tables) + len(delta_tables) == 0):
        detected_full = sorted(p.stem for p in Path("config/tables/full_load").glob("*.yaml"))
        detected_delta = sorted(p.stem for p in Path("config/tables/delta_load").glob("*.yaml"))
        log.error("No tables selected for the provided filters.")
        log.error("Detected full-load db configs: %s", detected_full)
        log.error("Detected delta-load db configs: %s", detected_delta)
        raise SystemExit(1)

    configured_databases = {
        item["name"]: item["env_database_key"] for item in databases_config.get("databases", [])
    }

    runtime_cfg = {
        "pipeline_name": settings["pipeline"]["name"],
        "dataset_name": settings["pipeline"]["dataset_name"],
        "bucket_url": get_bucket_url(settings),
        "snapshot_date": get_snapshot_date(settings),
        "pipelines_dir": get_pipelines_dir(),
        "full_load_layout": settings["destination"]["full_load_layout"],
        "delta_load_layout": settings["destination"]["delta_load_layout"],
        "full_load_delete_prefix_template": settings["destination"]["full_load_delete_prefix_template"],
        "file_format": settings["destination"]["file_format"],
        "delete_existing_full_load_snapshot": settings["snapshot"]["delete_existing_full_load_snapshot"],
        "extract_chunk_size": int(
            os.getenv(
                "EXTRACT_CHUNK_SIZE",
                str(settings.get("extract", {}).get("chunk_size", 100000)),
            )
        ),
        "extract_backend": str(settings.get("extract", {}).get("backend", "pyarrow")),
        # Each output table's S3 prefix is cleared at most once per job (replace prior run; avoid double-delete).
        "full_load_cleared_prefix_keys": set(),
        "full_load_delete_lock": threading.Lock(),
    }

    if os.getenv("PIPELINE_CLEAR_WORKDIR_BEFORE_RUN", "").lower() in ("1", "true", "yes"):
        clear_pipeline_workdir(runtime_cfg["pipelines_dir"], logger=log)

    max_full = int(os.getenv("MAX_WORKERS_FULL", "6"))
    max_delta = int(os.getenv("MAX_WORKERS_DELTA", "6"))
    run_mode = str(settings.get("execution", {}).get("run_mode", "full_then_delta"))
    debug_load_info = bool(run_database or run_table)

    engine_cache: dict[str, object] = {}
    engine_cache_lock = threading.Lock()

    full_success = full_failed = 0
    delta_success = delta_failed = 0
    full_rows = delta_rows = 0
    failed_tables: list[str] = []

    if run_mode in ("full_then_delta", "full_only"):
        serial_full_by_db = os.getenv("FULL_LOAD_DATABASE_SERIAL", "").lower() in ("1", "true", "yes")
        if serial_full_by_db:
            batches = _group_full_tables_by_database(full_tables)
            log.info(
                "FULL load: FULL_LOAD_DATABASE_SERIAL — %s database alias(es) run one after another; "
                "within each alias up to MAX_WORKERS_FULL=%s table(s) in parallel.",
                len(batches),
                max_full,
            )
            for db_alias, tables_in_db in batches:
                log.info(
                    "FULL load: starting database alias=%r (%s table(s))",
                    db_alias,
                    len(tables_in_db),
                )
                s, f, errs, rsum = run_phase(
                    "FULL",
                    tables_in_db,
                    max_workers=max_full,
                    worker_fn=run_one_full_table,
                    configured_databases=configured_databases,
                    runtime_cfg=runtime_cfg,
                    engine_cache=engine_cache,
                    engine_cache_lock=engine_cache_lock,
                    debug_load_info=debug_load_info,
                )
                full_success += s
                full_failed += f
                full_rows += rsum
                failed_tables.extend(errs)
                _dispose_engine_for_database_alias(
                    db_alias,
                    configured_databases,
                    engine_cache,
                    engine_cache_lock,
                )
                log.info("FULL load: completed database alias=%r", db_alias)
        else:
            s, f, errs, rsum = run_phase(
                "FULL",
                full_tables,
                max_workers=max_full,
                worker_fn=run_one_full_table,
                configured_databases=configured_databases,
                runtime_cfg=runtime_cfg,
                engine_cache=engine_cache,
                engine_cache_lock=engine_cache_lock,
                debug_load_info=debug_load_info,
            )
            full_success += s
            full_failed += f
            full_rows += rsum
            failed_tables.extend(errs)

    if run_mode in ("full_then_delta", "delta_only"):
        s, f, errs, rsum = run_phase(
            "DELTA",
            delta_tables,
            max_workers=max_delta,
            worker_fn=run_one_delta_table,
            configured_databases=configured_databases,
            runtime_cfg=runtime_cfg,
            engine_cache=engine_cache,
            engine_cache_lock=engine_cache_lock,
            debug_load_info=debug_load_info,
        )
        delta_success += s
        delta_failed += f
        delta_rows += rsum
        failed_tables.extend(errs)

    total_time = int((datetime.now() - overall_start).total_seconds())

    log.info("")
    log.info("=" * 80)
    log.info("RUN SUMMARY")
    log.info("=" * 80)
    log.info("Snapshot Date : %s", runtime_cfg["snapshot_date"])
    log.info("Bucket URL    : %s", runtime_cfg["bucket_url"])
    log.info("Dataset       : %s", runtime_cfg["dataset_name"])
    log.info("Full Layout   : %s", runtime_cfg["full_load_layout"])
    log.info("Delta Layout  : %s", runtime_cfg["delta_load_layout"])
    log.info("-" * 80)
    log.info(
        "FULL  -> Success: %s | Failed: %s | rows_loaded (normalize metrics): %s",
        full_success,
        full_failed,
        full_rows,
    )
    log.info(
        "DELTA -> Success: %s | Failed: %s | rows_loaded (normalize metrics): %s",
        delta_success,
        delta_failed,
        delta_rows,
    )
    log.info(
        "TOTAL rows (FULL+DELTA phases in this run): %s | TOTAL TIME: %ss",
        full_rows + delta_rows,
        total_time,
    )
    log.info(
        "RUN_SUMMARY_COMPLETE — FULL/DELTA phases finished; next lines are FAILED TABLES (if any) then SNS.",
    )
    flush_logging_handlers()

    if failed_tables:
        log.error("FAILED TABLES:")
        for entry in failed_tables:
            log.error("- %s", entry)
        flush_logging_handlers()
        delay_sec = int(os.getenv("SNS_PUBLISH_DELAY_SECONDS", "0").strip() or "0")
        if delay_sec > 0:
            log.info(
                "SNS_PUBLISH_DELAY_SECONDS=%s — pausing so CloudWatch can ingest RUN SUMMARY before SNS",
                delay_sec,
            )
            time.sleep(delay_sec)
            flush_logging_handlers()
        log.info(
            "Publishing SNS failure digest only after RUN SUMMARY (%s failed table(s)).",
            len(failed_tables),
        )
        flush_logging_handlers()
        publish_load_failures(
            failed_tables,
            pipeline_name=runtime_cfg["pipeline_name"],
            snapshot_date=runtime_cfg["snapshot_date"],
            bucket_url=runtime_cfg["bucket_url"],
            dataset_name=runtime_cfg["dataset_name"],
        )

    log.info("=" * 80)

    if full_failed + delta_failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()