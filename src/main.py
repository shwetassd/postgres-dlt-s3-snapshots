import os
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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

load_dotenv()
logging.getLogger("dlt").setLevel(logging.ERROR)
logging.getLogger("dlt.common").setLevel(logging.ERROR)
logging.getLogger("dlt.pipeline").setLevel(logging.ERROR)


def log_start(load_type: str, db: str, schema: str, table: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] [START {load_type}] {db}.{schema}.{table}")

def log_end(
    load_type: str,
    db: str,
    schema: str,
    table: str,
    start_time: datetime,
    *,
    success: bool,
    error: Exception | None = None,
) -> None:
    duration = int((datetime.now() - start_time).total_seconds())
    status = "SUCCESS" if success else "FAILED"
    msg = f"[END   {load_type}] {db}.{schema}.{table} | {status} | {duration}s"
    if error is not None:
        msg += f" | ERROR: {error}"
    print(msg)


def resolve_database_name(configured_databases: dict[str, str], database_alias: str) -> str:
    env_key = configured_databases.get(database_alias)
    if not env_key:
        raise ValueError(f"No env_database_key configured for database '{database_alias}'")
    actual_db = os.getenv(env_key)
    if not actual_db:
        raise ValueError(f"Environment variable '{env_key}' is not set")
    return actual_db


def get_engine_cached(engine_cache: dict[str, object], engine_cache_lock: threading.Lock, actual_db: str):
    with engine_cache_lock:
        engine = engine_cache.get(actual_db)
        if engine is None:
            engine = get_engine(actual_db)
            engine_cache[actual_db] = engine
        return engine


def build_pattern_path(bucket_url: str, dataset_name: str, prefix: str, snapshot_date: str) -> str:
    base = bucket_url.rstrip("/")
    dataset = dataset_name.strip("/")
    if dataset:
        return f"{base}/{dataset}/{prefix}/{{table_name}}/{snapshot_date}/"
    return f"{base}/{prefix}/{{table_name}}/{snapshot_date}/"


def run_one_full_table(table_cfg, configured_databases, runtime_cfg, engine_cache, engine_cache_lock):
    start_time = datetime.now()
    log_start("FULL", table_cfg.database, table_cfg.schema, table_cfg.table)

    actual_db = resolve_database_name(configured_databases, table_cfg.database)
    engine = get_engine_cached(engine_cache, engine_cache_lock, actual_db)

    if runtime_cfg["delete_existing_full_load_snapshot"]:
        delete_table_snapshot_prefix(
            bucket_url=runtime_cfg["bucket_url"],
            dataset_name=runtime_cfg["dataset_name"],
            layout_prefix_template=runtime_cfg["full_load_delete_prefix_template"],
            table_name=table_cfg.table,
            snapshot_date=runtime_cfg["snapshot_date"],
        )

    load_info = run_full_snapshot(
        engine=engine,
        pipeline_name=build_pipeline_name(
            runtime_cfg["pipeline_name"], "full", table_cfg.database, table_cfg.schema, table_cfg.table
        ),
        pipelines_dir=runtime_cfg["pipelines_dir"],
        dataset_name=runtime_cfg["dataset_name"],
        bucket_url=runtime_cfg["bucket_url"],
        layout=runtime_cfg["full_load_layout"],
        file_format=runtime_cfg["file_format"],
        schema_name=table_cfg.schema,
        table_name=table_cfg.table,
        columns=table_cfg.columns,
        select_sql=table_cfg.select_sql,
        output_columns=table_cfg.output_columns,
        snapshot_date=runtime_cfg["snapshot_date"],
        extract_chunk_size=runtime_cfg["extract_chunk_size"],
        extract_backend=runtime_cfg["extract_backend"],
    )

    return ("FULL", table_cfg.database, table_cfg.schema, table_cfg.table, start_time, load_info)


def run_one_delta_table(table_cfg, configured_databases, runtime_cfg, engine_cache, engine_cache_lock):
    start_time = datetime.now()
    log_start("DELTA", table_cfg.database, table_cfg.schema, table_cfg.table)

    actual_db = resolve_database_name(configured_databases, table_cfg.database)
    engine = get_engine_cached(engine_cache, engine_cache_lock, actual_db)

    load_info = run_delta_snapshot(
        engine=engine,
        pipeline_name=build_pipeline_name(
            runtime_cfg["pipeline_name"], "delta", table_cfg.database, table_cfg.schema, table_cfg.table
        ),
        pipelines_dir=runtime_cfg["pipelines_dir"],
        dataset_name=runtime_cfg["dataset_name"],
        bucket_url=runtime_cfg["bucket_url"],
        layout=runtime_cfg["delta_load_layout"],
        file_format=runtime_cfg["file_format"],
        schema_name=table_cfg.schema,
        table_name=table_cfg.table,
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

    return ("DELTA", table_cfg.database, table_cfg.schema, table_cfg.table, start_time, load_info)


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
    success = 0
    failed = 0
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(worker_fn, t, configured_databases, runtime_cfg, engine_cache, engine_cache_lock)
            for t in tables
        ]

        for future in as_completed(futures):
            try:
                load_type, db, schema, table, start_time, load_info = future.result()
                log_end(load_type, db, schema, table, start_time, success=True)
                if debug_load_info:
                    print(load_info)
                success += 1
            except Exception as e:
                print(f"[{phase_name}] FAILED | ERROR: {e}")
                failed += 1
                failures.append(str(e))

    return success, failed, failures


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

    print(f"Filters -> RUN_DATABASE={run_database}, RUN_TABLE={run_table}, RUN_LOAD_TYPE={run_load_type}")
    print(f"Selected -> FULL={len(full_tables)} table(s), DELTA={len(delta_tables)} table(s)")

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
        "extract_chunk_size": int(settings.get("extract", {}).get("chunk_size", 100000)),
        "extract_backend": str(settings.get("extract", {}).get("backend", "pyarrow")),
    }

    max_full = int(os.getenv("MAX_WORKERS_FULL", "8"))
    max_delta = int(os.getenv("MAX_WORKERS_DELTA", "6"))
    run_mode = str(settings.get("execution", {}).get("run_mode", "full_then_delta"))
    debug_load_info = bool(run_database or run_table)

    engine_cache: dict[str, object] = {}
    engine_cache_lock = threading.Lock()

    full_success = full_failed = 0
    delta_success = delta_failed = 0
    failed_tables: list[str] = []

    if run_mode in ("full_then_delta", "full_only"):
        s, f, errs = run_phase(
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
        failed_tables.extend(errs)

    if run_mode in ("full_then_delta", "delta_only"):
        s, f, errs = run_phase(
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
        failed_tables.extend(errs)

    total_time = int((datetime.now() - overall_start).total_seconds())

    print("\n" + "=" * 80)
    print("RUN SUMMARY")
    print("=" * 80)
    print(f"Snapshot Date : {runtime_cfg['snapshot_date']}")
    print(f"Bucket URL    : {runtime_cfg['bucket_url']}")
    print(f"Dataset       : {runtime_cfg['dataset_name']}")
    print(f"Full Layout   : {runtime_cfg['full_load_layout']}")
    print(f"Delta Layout  : {runtime_cfg['delta_load_layout']}")
    print("-" * 80)
    print(f"FULL  -> Success: {full_success} | Failed: {full_failed}")
    print(f"DELTA -> Success: {delta_success} | Failed: {delta_failed}")
    print(f"TOTAL TIME    : {total_time}s")

    if failed_tables:
        print("\nFAILED TABLES:")
        for f in failed_tables:
            print(f"- {f}")

    print("=" * 80)


if __name__ == "__main__":
    main()