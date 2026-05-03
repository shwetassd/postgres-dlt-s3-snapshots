# Full load vs delta load

## `RUN_LOAD_TYPE`

| Value | Behavior |
|-------|----------|
| *(unset)* | Same as **`both`**: **FULL phase first**, then **DELTA** (same `DLT_PIPELINES_DIR`). |
| `full` | Enabled tables from `config/tables/full_load/*.yaml` only. |
| `delta` | Enabled tables from `config/tables/delta_load/*.yaml` only. |
| `both` | **FULL phase first**, then **DELTA phase** in one process (same `DLT_PIPELINES_DIR`). |

Optional filters: `RUN_DATABASE`, `RUN_TABLE` (match YAML stem / table name).

## Incremental cursor (delta)

- **Operational tuning (workers, retries, fail_fast, logging, pools, …)** lives in **`config/settings.yaml`** under **`runtime:`** and **`postgres:`** (single source of defaults). Same **`UPPER_SNAKE`** environment variables as before override YAML when set to a non-empty value.
- **First-run floor:** `config/settings.yaml` → **`delta.initial_cursor_value`** (default Unix-era timestamp) is passed to dlt as **`initial_value`**, so the first extract uses **`WHERE cursor_column > that value`** instead of an unbounded scan. Per-table **`initial_value`** in `delta_load/*.yaml` overrides the global default when set. **`DELTA_INITIAL_CURSOR_VALUE`** env overrides YAML; set it empty to disable the floor globally.
- **Local:** Pipeline working dir **`DLT_PIPELINES_DIR`**, keyed by **pipeline name** (`…__delta__…`).
- **Destination (S3 / filesystem):** dlt **syncs pipeline state** to the bucket (folder **`_dlt_pipeline_state/`**, outside your `delta_load_layout`). If the local dir is empty or new, the next run may **restore** the incremental cursor from the bucket — clearing only local disk does not necessarily reset the watermark. See [Syncing state with destination](https://dlthub.com/docs/general-usage/state#syncing-state-with-destination) and [filesystem: syncing of dlt state](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#syncing-of-dlt-state).
- Parquet files under `delta_load/…/snapshot_date/…` are **data** outputs only; they are **not** what dlt uses as the incremental high-water mark (that lives in pipeline state).
- Full-load pipelines (`…__full__…`) and delta pipelines (`…__delta__…`) have **separate** state.

## Avoid wiping delta state

- **`PIPELINE_CLEAR_WORKDIR_BEFORE_RUN=1`** deletes **all** pipeline folders under `DLT_PIPELINES_DIR` locally only; **synced state on S3 may still restore** incremental cursors on the next run unless you also clear destination state (see README “Delta tables: one-time full extract again”). Use only when you understand the impact.
- **Log volume / CloudWatch / logging config** does **not** affect cursor state (cursors are files under `pipelines_dir`, not logs).

## Full then delta (always)

When **`RUN_LOAD_TYPE=both`** (or unset), the **DELTA** phase **always** runs after the **FULL** phase finishes, whether every full table succeeded or some failed.

## Parallelism

- **`MAX_WORKERS_FULL`** — parallel full table tasks (default `4`).
- **`MAX_WORKERS_DELTA`** — parallel delta table tasks (default `4`).
- Lower workers if tasks hit memory, DB connections, or S3 limits.

## Batch / ECS

- Persist **`DLT_PIPELINES_DIR`** on EFS or a durable volume if delta jobs must resume across runs.
- Same image; switch behavior with **`RUN_LOAD_TYPE`** and filters on the job definition.
