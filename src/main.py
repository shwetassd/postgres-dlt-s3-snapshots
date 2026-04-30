import gc
import os
import threading
import time
from datetime import datetime
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
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
from src.utils.run_summary_format import build_run_summary_box
from src.utils.transient_load_errors import (
    is_memory_or_disk_exhaustion,
    is_retryable_full_load_error,
)
from src.utils.workdir_cleanup import clear_pipeline_workdir

# Batch/ECS env must win over any .env baked into the image (override=False is the default).
load_dotenv(override=False)
silence_common_warnings()
log = configure_logging()
ensure_dlt_config_from_repo()


def log_start(load_type: str, db: str, schema: str, table: str) -> None:
    log.info("%-5s  start  %s.%s.%s", load_type, db, schema, table)


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
    status = "end" if success else "fail"
    # Empty dict {} means 0 rows written — must not treat as falsy (or we show rows=n/a incorrectly).
    tr: int | None = None
    if success and norm_counts is not None:
        tr = total_rows(norm_counts)
    if tr is not None and load_type == "DELTA" and tr == 0:
        row_part = "rows=0 (incremental: nothing newer than cursor)"
    elif tr is not None:
        row_part = f"rows={tr}"
    else:
        row_part = "rows=n/a" if success else ""
    extra = f"  {row_part}" if row_part else ""
    if success and norm_counts is not None and len(norm_counts) > 1:
        log.debug("%s.%s.%s per_resource counts: %s", db, schema, table, norm_counts)
    err_part = f"  error={error!r}" if error is not None else ""
    log.info(
        "%-5s  %-4s  %s.%s.%s  %4ds%s%s",
        load_type,
        status,
        db,
        schema,
        table,
        duration,
        extra,
        err_part,
    )


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


def _cleanup_before_full_load_retry(exc: BaseException, runtime_cfg: dict) -> None:
    """ gc.collect() always; optional wipe of DLT pipeline workdir on OOM/ENOSPC (see env). """
    gc.collect()
    if os.getenv("FULL_LOAD_RETRY_CLEAR_PIPELINE_WORKDIR", "").lower() not in ("1", "true", "yes"):
        return
    if not is_memory_or_disk_exhaustion(exc):
        return
    log.warning(
        "FULL_LOAD_RETRY_CLEAR_PIPELINE_WORKDIR: clearing %s after memory/disk pressure "
        "(unsafe if multiple table loads share this dir in parallel).",
        runtime_cfg["pipelines_dir"],
    )
    clear_pipeline_workdir(runtime_cfg["pipelines_dir"], logger=log)


def log_run_summary(
    *,
    runtime_cfg: dict,
    run_load_type: str,
    full_table_count: int,
    full_success: int,
    full_failed: int,
    full_rows: int,
    delta_table_count: int,
    delta_success: int,
    delta_failed: int,
    delta_rows: int,
    max_workers_delta: int,
    total_time_sec: int,
    run_database: str | None,
    run_table: str | None,
    max_workers_full: int,
    serial_full_by_db: bool,
    skipped_fail_fast: int = 0,
    reported_exit_code: int | None = None,
) -> None:
    """Emit one boxed block (single log record) so operators can scan CloudWatch easily."""
    box = build_run_summary_box(
        runtime_cfg=runtime_cfg,
        run_load_type=run_load_type,
        full_table_count=full_table_count,
        full_success=full_success,
        full_failed=full_failed,
        full_rows=full_rows,
        delta_table_count=delta_table_count,
        delta_success=delta_success,
        delta_failed=delta_failed,
        delta_rows=delta_rows,
        max_workers_delta=max_workers_delta,
        total_time_sec=total_time_sec,
        run_database=run_database,
        run_table=run_table,
        max_workers_full=max_workers_full,
        serial_full_by_db=serial_full_by_db,
        skipped_fail_fast=skipped_fail_fast,
        reported_exit_code=reported_exit_code,
    )
    log.info("\n%s", box)


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

    # Default 1: schema/SQL mistakes fail once; raise FULL_LOAD_MAX_ATTEMPTS for S3/network retries only.
    max_attempts = max(1, int(os.getenv("FULL_LOAD_MAX_ATTEMPTS", "1")))
    base_retry_delay = max(0.0, float(os.getenv("FULL_LOAD_RETRY_DELAY_SECONDS", "20")))
    skip_snapshot_delete = os.getenv("SKIP_DELETE_EXISTING_SNAPSHOT", "").lower() in (
        "1",
        "true",
        "yes",
    )

    load_info = None
    norm_counts = None
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        if runtime_cfg["delete_existing_full_load_snapshot"] and not skip_snapshot_delete:
            prefix_key = (
                f"{runtime_cfg['dataset_name']}|{runtime_cfg['snapshot_date']}|{output_table_name}"
            )
            cleared = runtime_cfg["full_load_cleared_prefix_keys"]
            lock = runtime_cfg["full_load_delete_lock"]
            with lock:
                if prefix_key not in cleared:
                    delete_table_snapshot_prefix(
                        bucket_url=runtime_cfg["bucket_url"],
                        dataset_name=runtime_cfg["dataset_name"],
                        layout_prefix_template=runtime_cfg["full_load_delete_prefix_template"],
                        table_name=output_table_name,
                        snapshot_date=runtime_cfg["snapshot_date"],
                    )
                    cleared.add(prefix_key)
                else:
                    log.debug(
                        "Skip duplicate S3 snapshot delete for output_table=%s "
                        "(prefix already cleared once this job)",
                        output_table_name,
                    )

        try:
            load_info, norm_counts = run_full_snapshot(
                engine=engine,
                pipeline_name=build_pipeline_name(
                    runtime_cfg["pipeline_name"],
                    "full",
                    table_cfg.database,
                    table_cfg.schema,
                    output_table_name,
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
            break
        except Exception as e:
            will_retry = (
                attempt < max_attempts
                and is_retryable_full_load_error(e)
            )
            if will_retry:
                log.warning(
                    "FULL load infra/transient error %s.%s.%s (attempt %s/%s), retrying after %.1fs: %s",
                    table_cfg.database,
                    table_cfg.schema,
                    output_table_name,
                    attempt,
                    max_attempts,
                    base_retry_delay * (2 ** (attempt - 1)),
                    e,
                )
                _cleanup_before_full_load_retry(e, runtime_cfg)
                if runtime_cfg["delete_existing_full_load_snapshot"] and not skip_snapshot_delete:
                    prefix_key = (
                        f"{runtime_cfg['dataset_name']}|{runtime_cfg['snapshot_date']}|{output_table_name}"
                    )
                    lock = runtime_cfg["full_load_delete_lock"]
                    with lock:
                        runtime_cfg["full_load_cleared_prefix_keys"].discard(prefix_key)
                time.sleep(base_retry_delay * (2 ** (attempt - 1)))
                continue
            raise

    assert load_info is not None and norm_counts is not None

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

    max_attempts = max(
        1,
        int(os.getenv("DELTA_LOAD_MAX_ATTEMPTS", os.getenv("FULL_LOAD_MAX_ATTEMPTS", "1"))),
    )
    base_retry_delay = max(
        0.0,
        float(os.getenv("DELTA_LOAD_RETRY_DELAY_SECONDS", os.getenv("FULL_LOAD_RETRY_DELAY_SECONDS", "20"))),
    )

    load_info = None
    norm_counts = None
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            load_info, norm_counts = run_delta_snapshot(
                engine=engine,
                pipeline_name=build_pipeline_name(
                    runtime_cfg["pipeline_name"],
                    "delta",
                    table_cfg.database,
                    table_cfg.schema,
                    output_table_name,
                ),
                pipelines_dir=runtime_cfg["pipelines_dir"],
                dataset_name=runtime_cfg["dataset_name"],
                bucket_url=runtime_cfg["bucket_url"],
                layout=runtime_cfg["delta_load_layout"],
                file_format=runtime_cfg["file_format"],
                schema_name=table_cfg.schema,
                source_table_name=table_cfg.table,
                table_name=output_table_name,
                cursor_column=table_cfg.cursor_column,
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
                initial_value=table_cfg.initial_value,
                cursor_expression=table_cfg.cursor_expression,
            )
            break
        except Exception as e:
            will_retry = attempt < max_attempts and is_retryable_full_load_error(e)
            if will_retry:
                log.warning(
                    "DELTA load infra/transient error %s.%s.%s (attempt %s/%s), retrying after %.1fs: %s",
                    table_cfg.database,
                    table_cfg.schema,
                    output_table_name,
                    attempt,
                    max_attempts,
                    base_retry_delay * (2 ** (attempt - 1)),
                    e,
                )
                _cleanup_before_full_load_retry(e, runtime_cfg)
                time.sleep(base_retry_delay * (2 ** (attempt - 1)))
                continue
            raise

    assert load_info is not None and norm_counts is not None

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
    skipped_fail_fast = 0
    fail_fast = (
        phase_name == "FULL"
        and os.getenv("FULL_LOAD_FAIL_FAST", "").lower() in ("1", "true", "yes")
    ) or (
        phase_name == "DELTA"
        and os.getenv("DELTA_LOAD_FAIL_FAST", "").lower() in ("1", "true", "yes")
    )
    fail_fast_cancel_done = False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_cfg = {
            executor.submit(
                worker_fn, t, configured_databases, runtime_cfg, engine_cache, engine_cache_lock
            ): t
            for t in tables
        }
        ff_note = " fail_fast" if fail_fast else ""
        log.info(
            "%s phase: %s table(s)   workers=%s%s",
            phase_name,
            len(tables),
            max_workers,
            ff_note,
        )
        if phase_name == "FULL" and runtime_cfg.get("delete_existing_full_load_snapshot"):
            if os.getenv("SKIP_DELETE_EXISTING_SNAPSHOT", "").lower() in ("1", "true", "yes"):
                log.info(
                    "FULL: snapshot_date=%s — not deleting existing S3 objects (SKIP_DELETE_EXISTING_SNAPSHOT)",
                    runtime_cfg["snapshot_date"],
                )
            else:
                log.debug(
                    "FULL: replacing each table prefix under snapshot_date=%s before extract",
                    runtime_cfg["snapshot_date"],
                )
        if phase_name == "FULL":
            log.debug(
                "FULL: long stalls usually mean SQL/I/O in flight or OOM; check PG_STATEMENT_TIMEOUT_MS / ECS",
            )
            if fail_fast:
                log.info("FULL: fail_fast — pending tasks cancel after first failure (running tasks finish)")
        if phase_name == "DELTA" and fail_fast:
            log.info("DELTA: fail_fast — pending tasks cancel after first failure (running tasks finish)")
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
                        log.debug("load_info=%s", load_info)
                    if not norm_counts:
                        log.debug(
                            "No normalize row_counts in trace for %s.%s.%s (dlt may omit in some paths)",
                            db,
                            schema,
                            table,
                        )
                    success += 1
                except CancelledError:
                    skipped_fail_fast += 1
                    ff_env = (
                        "FULL_LOAD_FAIL_FAST"
                        if phase_name == "FULL"
                        else "DELTA_LOAD_FAIL_FAST"
                        if phase_name == "DELTA"
                        else "FAIL_FAST"
                    )
                    log.info(
                        "[%s] skipped (%s): %s.%s.%s — pending task cancelled "
                        "after another table failed.",
                        phase_name,
                        ff_env,
                        cfg.database,
                        cfg.schema,
                        out_name,
                    )
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
                    if fail_fast and not fail_fast_cancel_done:
                        fail_fast_cancel_done = True
                        ncancel = 0
                        for f in future_to_cfg:
                            if not f.done() and f.cancel():
                                ncancel += 1
                        still_running = sum(1 for f in future_to_cfg if not f.done())
                        ff_env = (
                            "FULL_LOAD_FAIL_FAST"
                            if phase_name == "FULL"
                            else "DELTA_LOAD_FAIL_FAST"
                            if phase_name == "DELTA"
                            else "FAIL_FAST"
                        )
                        log.warning(
                            "[%s] %s: cancelled %s pending table load(s); "
                            "%s worker(s) still active until they finish.",
                            phase_name,
                            ff_env,
                            ncancel,
                            still_running,
                        )
        finally:
            stop_watchdog.set()

    log.info(
        "%s phase done: ok=%s failed=%s rows=%s",
        phase_name,
        success,
        failed,
        rows_total,
    )
    if failed:
        log.error(
            "%s phase: %s table load(s) FAILED — traceback is logged above per failed table. "
            "Process exit code depends on EXIT_ON_TABLE_FAILURES (see RUN SUMMARY).",
            phase_name,
            failed,
        )
    if failed or skipped_fail_fast:
        log.debug("%s phase: failures logged above; RUN SUMMARY follows", phase_name)

    return success, failed, failures, rows_total, skipped_fail_fast


def main() -> None:
    overall_start = datetime.now()

    settings = load_settings()
    databases_config = load_database_config()

    # Distinguish unset vs empty: unset → full; empty string → invalid below.
    _raw_run_load_type = os.environ.get("RUN_LOAD_TYPE")
    if _raw_run_load_type is None:
        run_load_type = "full"
    else:
        run_load_type = _raw_run_load_type.strip().lower()
    log.debug(
        "RUN_LOAD_TYPE raw=%r resolved=%r",
        _raw_run_load_type,
        run_load_type,
    )
    if run_load_type not in ("full", "delta", "both"):
        log.error("RUN_LOAD_TYPE must be one of: full, delta, both (got %r)", run_load_type)
        raise SystemExit(2)

    full_tables = load_full_load_tables() if run_load_type in ("full", "both") else []
    delta_tables = load_delta_load_tables() if run_load_type in ("delta", "both") else []

    run_database = os.getenv("RUN_DATABASE")
    run_table = os.getenv("RUN_TABLE")

    if run_database:
        full_tables = [t for t in full_tables if t.database == run_database]
        delta_tables = [t for t in delta_tables if t.database == run_database]

    if run_table:
        full_tables = [t for t in full_tables if t.table == run_table]
        delta_tables = [t for t in delta_tables if t.table == run_table]

    n_full = len(full_tables)
    n_delta = len(delta_tables)
    log.info(
        "run   mode=%s   tables   full=%s   delta=%s   RUN_DATABASE=%s   RUN_TABLE=%s",
        run_load_type,
        n_full,
        n_delta,
        run_database or "*",
        run_table or "*",
    )

    delta_load_dir = Path("config/tables/delta_load")
    detected_delta = (
        sorted(p.stem for p in delta_load_dir.glob("*.yaml")) if delta_load_dir.is_dir() else []
    )

    if run_database or run_table:
        if n_full == 0 and n_delta == 0:
            detected_full = sorted(p.stem for p in Path("config/tables/full_load").glob("*.yaml"))
            log.error("No tables selected for the provided filters.")
            log.error("Detected full-load db configs: %s", detected_full)
            log.error("Detected delta-load db configs: %s", detected_delta)
            raise SystemExit(1)
    else:
        if run_load_type == "full" and n_full == 0:
            log.error("RUN_LOAD_TYPE=full but no enabled full-load tables.")
            raise SystemExit(1)
        if run_load_type == "delta" and n_delta == 0:
            log.error("RUN_LOAD_TYPE=delta but no enabled delta-load tables.")
            raise SystemExit(1)
        if run_load_type == "both" and n_full == 0 and n_delta == 0:
            log.error("RUN_LOAD_TYPE=both but no enabled full- or delta-load tables.")
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
        "full_load_delete_prefix_template": settings["destination"]["full_load_delete_prefix_template"],
        "delta_load_layout": settings["destination"].get(
            "delta_load_layout",
            "delta_load/{table_name}/{snapshot_date}/{load_id}.{file_id}.{ext}",
        ),
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
        if n_delta > 0:
            log.warning(
                "PIPELINE_CLEAR_WORKDIR_BEFORE_RUN removed pipeline state under %s — incremental "
                "(delta) cursors were wiped; delta extracts in this job behave like first-time loads.",
                runtime_cfg["pipelines_dir"],
            )

    # Default 4: many parallel dlt→S3 uploads (per table) can hit connection / Content-Length issues
    # at high MAX_WORKERS_FULL; override in env if the job is small or S3 is very quiet.
    max_full = int(os.getenv("MAX_WORKERS_FULL", "4"))
    max_delta = int(os.getenv("MAX_WORKERS_DELTA", "4"))
    if n_full >= 50 and max_full > 6:
        log.info(
            "Many tables (%s) with MAX_WORKERS_FULL=%s — if S3 upload errors persist, try 3–4.",
            n_full,
            max_full,
        )
    debug_load_info = bool(run_database or run_table)

    if run_load_type == "both":
        log.info(
            "both: FULL phase then DELTA (same process, dlt state under %s)",
            runtime_cfg["pipelines_dir"],
        )

    engine_cache: dict[str, object] = {}
    engine_cache_lock = threading.Lock()

    full_success = full_failed = 0
    full_rows = 0
    full_skipped_fail_fast = 0
    delta_success = delta_failed = 0
    delta_rows = 0
    delta_skipped_fail_fast = 0
    failed_tables: list[str] = []

    serial_full_by_db = os.getenv("FULL_LOAD_DATABASE_SERIAL", "").lower() in ("1", "true", "yes")
    if run_load_type in ("full", "both") and n_full > 0:
        if serial_full_by_db:
            batches = _group_full_tables_by_database(full_tables)
            log.info(
                "FULL: SERIAL_BY_DB — %s database group(s), workers=%s parallel within each group",
                len(batches),
                max_full,
            )
            for db_alias, tables_in_db in batches:
                log.info("FULL: group %r (%s table(s))", db_alias, len(tables_in_db))
                s, f, errs, rsum, skip_ff = run_phase(
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
                full_skipped_fail_fast += skip_ff
                failed_tables.extend(errs)
                _dispose_engine_for_database_alias(
                    db_alias,
                    configured_databases,
                    engine_cache,
                    engine_cache_lock,
                )
                log.debug("FULL: finished group %r", db_alias)
        else:
            s, f, errs, rsum, skip_ff = run_phase(
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
            full_skipped_fail_fast += skip_ff
            failed_tables.extend(errs)

    skip_delta_phase = (
        run_load_type == "both"
        and n_delta > 0
        and full_failed > 0
        and os.getenv("SKIP_DELTA_WHEN_FULL_FAILS", "").lower() in ("1", "true", "yes")
    )
    if skip_delta_phase:
        log.warning(
            "SKIP_DELTA_WHEN_FULL_FAILS: FULL had %s failure(s) — skipping DELTA phase this run.",
            full_failed,
        )
    elif run_load_type == "both" and n_delta > 0 and full_failed > 0:
        log.warning(
            "FULL had %s failure(s); DELTA phase still runs. "
            "Set SKIP_DELTA_WHEN_FULL_FAILS=1 to skip DELTA after FULL failures.",
            full_failed,
        )

    if run_load_type in ("delta", "both") and n_delta > 0 and not skip_delta_phase:
        s, f, errs, rsum, skip_ff = run_phase(
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
        delta_skipped_fail_fast += skip_ff
        failed_tables.extend(errs)

    total_time = int((datetime.now() - overall_start).total_seconds())

    exit_on_table_failures = os.getenv("EXIT_ON_TABLE_FAILURES", "").lower() in (
        "1",
        "true",
        "yes",
    )
    reported_exit_code = (
        1
        if (full_failed > 0 or delta_failed > 0) and exit_on_table_failures
        else 0
    )

    log_run_summary(
        runtime_cfg=runtime_cfg,
        run_load_type=run_load_type,
        full_table_count=n_full,
        full_success=full_success,
        full_failed=full_failed,
        full_rows=full_rows,
        delta_table_count=n_delta,
        delta_success=delta_success,
        delta_failed=delta_failed,
        delta_rows=delta_rows,
        max_workers_delta=max_delta,
        total_time_sec=total_time,
        run_database=run_database,
        run_table=run_table,
        max_workers_full=max_full,
        serial_full_by_db=serial_full_by_db,
        skipped_fail_fast=full_skipped_fail_fast + delta_skipped_fail_fast,
        reported_exit_code=reported_exit_code,
    )
    flush_logging_handlers()

    if failed_tables:
        log.info("")
        log.error("FAILED TABLES (%s)", len(failed_tables))
        for entry in failed_tables:
            log.error("  - %s", entry)
        flush_logging_handlers()
        delay_sec = int(os.getenv("SNS_PUBLISH_DELAY_SECONDS", "0").strip() or "0")
        if delay_sec > 0:
            log.info(
                "SNS_PUBLISH_DELAY_SECONDS=%s — pausing so CloudWatch can ingest RUN SUMMARY before SNS",
                delay_sec,
            )
            time.sleep(delay_sec)
            flush_logging_handlers()
        batch_attempt = (os.getenv("AWS_BATCH_JOB_ATTEMPT") or "").strip()
        attempt_no = int(batch_attempt) if batch_attempt.isdigit() else 1
        force_digest = os.getenv("SNS_ALWAYS_PUBLISH_FAILURE_DIGEST", "").lower() in (
            "1",
            "true",
            "yes",
        )
        # Batch re-runs the container on failure — each run would publish again unless we skip repeats.
        skip_duplicate_retry = (
            attempt_no > 1
            and not force_digest
            and os.getenv("SNS_PUBLISH_FAILURE_DIGEST_ON_BATCH_RETRY_ONLY_ONCE", "1").lower()
            not in ("0", "false", "no")
        )
        if skip_duplicate_retry:
            log.info(
                "Skipping SNS failure digest on Batch retry (AWS_BATCH_JOB_ATTEMPT=%s) — "
                "already published on attempt 1. Every attempt: SNS_ALWAYS_PUBLISH_FAILURE_DIGEST=1. "
                "Disable skip: SNS_PUBLISH_FAILURE_DIGEST_ON_BATCH_RETRY_ONLY_ONCE=0.",
                batch_attempt or "?",
            )
        else:
            log.info(
                "Publishing SNS failure digest (%s failed table(s)).",
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

    # Per-table failures do not stop other tables (unless FULL_LOAD_FAIL_FAST / DELTA_LOAD_FAIL_FAST).
    # Default: exit 0 so the batch job is "successful" while SNS (if configured) lists failed tables.
    if full_failed > 0 or delta_failed > 0:
        if exit_on_table_failures:
            log.error(
                "EXIT_ON_TABLE_FAILURES: exiting with code 1 (%s FULL failure(s), %s DELTA failure(s)).",
                full_failed,
                delta_failed,
            )
            raise SystemExit(1)
        log.warning(
            "Table load failures: FULL=%s DELTA=%s — exiting with code 0. "
            "SNS failure digest already sent if SNS_FAILURE_TOPIC_ARN is set. "
            "Set EXIT_ON_TABLE_FAILURES=1 to exit 1 for orchestration that must fail the task.",
            full_failed,
            delta_failed,
        )


if __name__ == "__main__":
    main()