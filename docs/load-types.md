# Full load vs delta load

## `RUN_LOAD_TYPE`

| Value | Behavior |
|-------|----------|
| `full` (default if unset) | Enabled tables from `config/tables/full_load/*.yaml` only. |
| `delta` | Enabled tables from `config/tables/delta_load/*.yaml` only. |
| `both` | **FULL phase first**, then **DELTA phase** in one process (same `DLT_PIPELINES_DIR`). |

Optional filters: `RUN_DATABASE`, `RUN_TABLE` (match YAML stem / table name).

## Incremental cursor (delta)

- Stored under **`DLT_PIPELINES_DIR`** on disk, keyed by **pipeline name** (`…__delta__…`), **not** by S3 paths.
- Parquet files under `delta_load/…/snapshot_date/…` are outputs only; they do **not** replace cursor state.
- Full-load pipelines (`…__full__…`) and delta pipelines (`…__delta__…`) have **separate** state.

## Avoid wiping delta state

- **`PIPELINE_CLEAR_WORKDIR_BEFORE_RUN=1`** deletes **all** pipeline folders under `DLT_PIPELINES_DIR`, including delta cursors. Use only when you intentionally reset (or on a fresh ephemeral task where state does not need to persist).
- **Log volume / CloudWatch / logging config** does **not** affect cursor state (cursors are files under `pipelines_dir`, not logs).

## Optional: skip delta after full failures

When `RUN_LOAD_TYPE=both`, set **`SKIP_DELTA_WHEN_FULL_FAILS=1`** to skip the DELTA phase if the FULL phase had any failures (reduces redundant work). Default is off: DELTA still runs unless you set this.

## Parallelism

- **`MAX_WORKERS_FULL`** — parallel full table tasks (default `4`).
- **`MAX_WORKERS_DELTA`** — parallel delta table tasks (default `4`).
- Lower workers if tasks hit memory, DB connections, or S3 limits.

## Batch / ECS

- Persist **`DLT_PIPELINES_DIR`** on EFS or a durable volume if delta jobs must resume across runs.
- Same image; switch behavior with **`RUN_LOAD_TYPE`** and filters on the job definition.
