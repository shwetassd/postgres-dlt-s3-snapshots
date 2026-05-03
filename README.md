# postgres-dlt-s3

Loads selected **PostgreSQL** tables into **Parquet** on **S3** (or any URL `dlt` filesystem supports), using [`dlt`](https://dlthub.com/). Each run uses a **snapshot date** (today by default) in object paths.

**Modes**

- **Full load**: snapshot per output table for that date (optional replace of the table’s prefix on S3).
- **Delta load**: cursor-based incremental extracts; pipeline state under `DLT_PIPELINES_DIR` **and** (with filesystem/S3) synced under **`_dlt_pipeline_state/`** on the bucket — see [Delta tables: one-time full extract again](#delta-tables-one-time-full-extract-again).

## How it works

1. `python -m src.main` reads `config/settings.yaml`, `config/databases.yaml`, and every `config/tables/full_load/*.yaml` / `delta_load/*.yaml`.
2. Enabled tables are scheduled into a **FULL** phase and/or a **DELTA** phase (`RUN_LOAD_TYPE`).
3. Workers connect with SQLAlchemy/psycopg2, stream chunks (pandas), then **dlt** writes Parquet under the configured layout.
4. Optional filters **`RUN_DATABASE`** / **`RUN_TABLE`** limit work to one alias / one source table name.

Database **alias** = YAML filename stem (e.g. `rfq_service` → `config/tables/full_load/rfq_service.yaml`). **`RUN_TABLE`** matches the source `table:` field inside that YAML.

## Where it runs

- **Locally**: repo root, venv; optional `.env` loaded by `python-dotenv` unless **`SKIP_DOTENV=1`** (unset locally; **set in the Docker image** so only injected env/secrets apply).
- **AWS Batch / ECS / CI**: same entrypoint `python -m src.main`; inject env or Secrets Manager → env (Postgres, bucket, `POSTGRES_DB_*`, optional SNS). Use a **persistent** `DLT_PIPELINES_DIR` (e.g. EFS) only if you need local pipeline folders to survive across tasks; ephemeral disks still work if you rely on **dlt state synced to S3** under `_dlt_pipeline_state/`.
- **Docker**: multi-stage **`Dockerfile`** (Python 3.13-slim), non-root user, **`SKIP_DOTENV=1`**. Build: `docker build -t postgres-dlt-s3 .` from repo root.

At startup, **`config/dlt/config.toml`** is copied to **`.dlt/config.toml`** (gitignored) so dlt picks up project settings; edit the file under **`config/dlt/`**, not the generated copy.

## Failure notifications

If **`SNS_FAILURE_TOPIC_ARN`** is set, after a run with failed tables the app publishes a **failure digest** to that SNS topic (IAM `sns:Publish` required). Optional: `SNS_PUBLISH_DELAY_SECONDS`, `SNS_ALWAYS_PUBLISH_FAILURE_DIGEST`, `SNS_PUBLISH_FAILURE_DIGEST_ON_BATCH_RETRY_ONLY_ONCE` (see `src/main.py` / `src/utils/sns_notify.py`). No topic → logs only.

## Setup (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

For local runs you can copy a `.env` pattern from your team **or** export the same variables in your shell. Required pieces:

## Essential environment

| Area | Variables |
|------|-----------|
| Postgres | `PG_HOST`, `PG_PORT` (default `5432`), `PG_USER`, `PG_PASSWORD` |
| Postgres (RDS) | Optional **`PG_SSLMODE`** (e.g. **`require`**) — appended to the SQLAlchemy URL |
| DB names | One env per alias in `config/databases.yaml`, e.g. `POSTGRES_DB_RFQ_SERVICE`, `POSTGRES_DB_SUPPLIER_OFFERS` |
| Destination | **`DESTINATION__FILESYSTEM__BUCKET_URL`** = `s3://bucket/prefix` **when set** (non-empty env wins first). **Or** omit env and set **`destination.bucket_url`** in [`config/settings.yaml`](config/settings.yaml). |
| AWS (S3/SNS) | Task/instance **IAM role** (preferred on Batch) or standard credential chain; region set by the runtime |

**Runtime**

- **Job tuning defaults** (workers, retries, `fail_fast`, watchdog interval, extract chunk/backend, pipeline dir, Postgres pool sizes, log level, SNS timing, optional JSON run summary upload, …): edit **`runtime:`** and **`postgres:`** in [`config/settings.yaml`](config/settings.yaml). **Uppercase env vars** with the documented names still **override** YAML when set to a non-empty value.
- **`EXIT_ON_TABLE_FAILURES`**: set to `1` / `true` / `yes` so the process exits **1** when any table fails (useful when orchestration must mark the job failed). Default in YAML is **false** (failures logged + optional SNS; exit **0** unless this is enabled).
- **`RUN_SUMMARY_S3_UPLOAD`** / YAML **`runtime.run_summary_s3_upload`**: when enabled, writes a JSON summary to `{dataset}/_batch_run_summaries/{snapshot_date}/{job_id}.json` on the same bucket root as the destination URL (see `src/utils/run_summary_s3.py`).
- **`DELTA_INITIAL_CURSOR_VALUE`**: optional env override for first-run incremental floor; empty disables global floor (per-table `initial_value` in delta YAML still applies). Default floor comes from **`delta.initial_cursor_value`** in settings.
- `RUN_LOAD_TYPE`: **`both`** (full then delta) if unset; set to **`full`** or **`delta`** to run only that phase. Details: [`docs/load-types.md`](docs/load-types.md).
- `RUN_DATABASE`, `RUN_TABLE`: optional filters (see above).

## Run commands (local)

From repo root:

```bash
# Full phase then delta (default when RUN_LOAD_TYPE is unset)
python -m src.main

# Full load only
RUN_LOAD_TYPE=full python -m src.main

# Delta load only
RUN_LOAD_TYPE=delta python -m src.main

# One database alias (all its tables in scope for that mode)
RUN_DATABASE=rfq_service python -m src.main

# One source table (optionally pin database)
RUN_TABLE=recommended_supplier python -m src.main
RUN_DATABASE=rfq_service RUN_TABLE=recommended_supplier python -m src.main
```

## Same-day reruns: full load vs delta

Both phases put **`snapshot_date`** in the S3 path (from `config/settings.yaml` → `snapshot`, usually **today’s date**). If you run the job **twice the same calendar day**, behavior differs by phase.

| | **Full load** | **Delta load** |
|--|----------------|----------------|
| **S3 before writing** | When `snapshot.delete_existing_full_load_snapshot` is **`true`** (default), the app deletes the prefix `full_load/{table_name}/{snapshot_date}/` (see `full_load_delete_prefix_template`), then loads fresh Parquet for that run. | **No** delete step. Existing objects under `delta_load/{table_name}/{snapshot_date}/` are left in place. |
| **Second run same day** | **Replaces** that day’s full snapshot for each table (same folder; new files after delete). | **Adds** new Parquet objects (new **`load_id`** / file ids in the layout). Older files from earlier runs the same day remain. |
| **What gets extracted** | Full table (for configured columns) each time. | Second run uses the **incremental cursor**: only rows with **`dlt_cursor`** (or your YAML cursor) **greater than** the saved high-water mark. Often **zero new rows** if nothing changed. |
| **Duplicate business keys in S3** | Typically one row set per key for that snapshot after the replace (that run’s dump). | **Possible across runs**: updates that move the watermark forward cause the row to be emitted **again** in a **new** file; downstream jobs usually **dedupe** by primary key + latest timestamp. |

Layouts are defined in `settings.yaml`: `full_load_layout` vs `delta_load_layout` (delta is explicitly **append / new files per run**, not replace-before-load).

## Delta tables: one-time full extract again

Delta loads stay incremental using a **cursor** carried in **dlt pipeline state**. Two places matter for this project:

1. **Local working dir:** `DLT_PIPELINES_DIR` (default **`.dlt_work`** — see `src/utils/dlt_runtime.py`). The run summary **`dlt pipelines_dir=...`** is the path to use when deleting folders there.
2. **Destination (S3):** With the **filesystem** destination, dlt [**syncs pipeline state to the bucket**](https://dlthub.com/docs/general-usage/state#syncing-state-with-destination) under **`_dlt_pipeline_state/`** (that path does **not** follow `delta_load_layout` in `settings.yaml`). After a clean local dir, the next run often **restores** incremental state from S3, so logs still show **`incremental … cutoff_exclusive=…`** — not a full-table extract.

So: **empty `delta_load/...` parquet prefixes do not prove “first run,”** and **clearing only `/tmp/dlt` is not enough** if synced state still exists on the bucket.

**When you need “run everything once more” for a delta table** (true backfill), reset **both** local pipeline state for that table **and** the synced destination state for that pipeline (or use [`dlt pipeline drop`](https://dlthub.com/docs/reference/command-line-interface.md#dlt-pipeline-drop) / drop dataset — see dlt docs). Details: [`docs/load-types.md`](docs/load-types.md).

**Typical steps**

1. **Remove the local pipeline folder** for that delta table. Directory name:

   `{pipeline.name}__delta__{database_alias}__{schema}__{output_table}`

   (`pipeline.name` from `config/settings.yaml`; database alias / schema / output table from `config/tables/delta_load/<alias>.yaml`.)

   ```bash
   DIR="${DLT_PIPELINES_DIR:-.dlt_work}"
   ls "$DIR" | grep message   # optional

   rm -rf "$DIR/postgres_dlt_s3__delta__conversations__public__message"
   ```

2. **Reset synced state on S3** for that pipeline + dataset (`dataset_name` in `settings.yaml`, same bucket as `DESTINATION__FILESYSTEM__BUCKET_URL`). At minimum, remove or archive the matching entries under **`_dlt_pipeline_state/`** on that bucket (see [filesystem destination — syncing of dlt state](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#syncing-of-dlt-state)). If you skip this step, the next run can **reload the old incremental cursor** from the bucket.

3. **Rerun delta:**

   ```bash
   RUN_LOAD_TYPE=delta RUN_DATABASE=conversations RUN_TABLE=message python -m src.main
   ```

   You should see **`delta extract … full_table …`** with **`loading_all_rows`** (not **`incremental cutoff_exclusive=…`**) when there is no restored cursor.

**Optional:** If you **always** keep a persistent `DLT_PIPELINES_DIR` and do not want restores from the bucket, dlt documents setting **`restore_from_destination=false`** in config — see [state: syncing with destination](https://dlthub.com/docs/general-usage/state#syncing-state-with-destination). That tradeoff is wrong for ephemeral tasks that rely on S3 to remember the cursor.

**Broader reset:** `PIPELINE_CLEAR_WORKDIR_BEFORE_RUN=1` wipes **all** local pipeline folders under `DLT_PIPELINES_DIR` only; it does **not** remove `_dlt_pipeline_state` on S3. Use only when you understand the impact.

**Full snapshots vs delta-only:** [`RUN_LOAD_TYPE=both`](docs/load-types.md) runs configured **full_load** tables, then **delta_load** tables. A table listed **only** under `delta_load/` never gets a separate full-load phase unless you add it to `full_load/` or rely on delta’s first run after clearing **local and destination** pipeline state as above.

## Configuration layout

| Path | Role |
|------|------|
| `config/settings.yaml` | Pipeline name, dataset, S3 layouts (and optional **`destination.bucket_url`**), snapshot date mode, **`runtime`** / **`postgres`** tuning, **`delta`** initial cursor, **`extract`** defaults |
| `config/dlt/config.toml` | dlt performance/schema naming — synced to `.dlt/config.toml` at process start |
| `config/databases.yaml` | Alias → env key for real Postgres database name |
| `config/tables/full_load/<alias>.yaml` | Full-load table definitions |
| `config/tables/delta_load/<alias>.yaml` | Delta tables (`cursor_column`, etc.) |

## Output layout

Parquet paths follow `destination.full_load_layout` / `delta_load_layout` in `settings.yaml` (placeholders include `table_name`, `snapshot_date`, `load_id`, …). Full loads can delete that table’s snapshot prefix for the date when enabled in settings.

Do not commit secrets. Prefer IAM roles on AWS; locally keep `.env` gitignored or avoid `.env` entirely and use shell/env tooling.

## Requirements

- **Python 3.13** (see `Dockerfile` / local venv)
- Network reachability from the runner to Postgres and to S3 (when using `s3://` destinations)
