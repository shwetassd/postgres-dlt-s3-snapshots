# postgres-dlt-s3

Loads selected **PostgreSQL** tables into **Parquet** on **S3** (or any URL `dlt` filesystem supports), using [`dlt`](https://dlthub.com/). Each run uses a **snapshot date** (today by default) in object paths.

**Modes**

- **Full load**: snapshot per output table for that date (optional replace of the table’s prefix on S3).
- **Delta load**: cursor-based incremental extracts; state lives under `DLT_PIPELINES_DIR` (persist this on scheduled jobs or every delta behaves like a first run).

## How it works

1. `python -m src.main` reads `config/settings.yaml`, `config/databases.yaml`, and every `config/tables/full_load/*.yaml` / `delta_load/*.yaml`.
2. Enabled tables are scheduled into a **FULL** phase and/or a **DELTA** phase (`RUN_LOAD_TYPE`).
3. Workers connect with SQLAlchemy/psycopg2, stream chunks (pandas), then **dlt** writes Parquet under the configured layout.
4. Optional filters **`RUN_DATABASE`** / **`RUN_TABLE`** limit work to one alias / one source table name.

Database **alias** = YAML filename stem (e.g. `rfq_service` → `config/tables/full_load/rfq_service.yaml`). **`RUN_TABLE`** matches the source `table:` field inside that YAML.

## Where it runs

- **Locally**: repo root, venv, `.env` via `python-dotenv`.
- **AWS Batch / ECS / CI**: same entrypoint; inject env (Postgres, bucket, `POSTGRES_DB_*`, optional SNS). Use a **persistent** `DLT_PIPELINES_DIR` (e.g. EFS) if deltas must keep cursors across tasks.

## Failure notifications

If **`SNS_FAILURE_TOPIC_ARN`** is set, after a run with failed tables the app publishes a **failure digest** to that SNS topic (IAM `sns:Publish` required). Optional: `SNS_PUBLISH_DELAY_SECONDS`, `SNS_ALWAYS_PUBLISH_FAILURE_DIGEST`, `SNS_PUBLISH_FAILURE_DIGEST_ON_BATCH_RETRY_ONLY_ONCE` (see `src/main.py` / `src/utils/sns_notify.py`). No topic → logs only.

## Setup (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env` pattern from your team; required pieces below.

## Essential environment

| Area | Variables |
|------|-----------|
| Postgres | `PG_HOST`, `PG_PORT` (default `5432`), `PG_USER`, `PG_PASSWORD` |
| DB names | One env per alias in `config/databases.yaml`, e.g. `POSTGRES_DB_RFQ_SERVICE`, `POSTGRES_DB_SUPPLIER_OFFERS` |
| Destination | `DESTINATION__FILESYSTEM__BUCKET_URL` (must match `settings.yaml`) |
| AWS (S3/SNS) | Standard credential/region setup; role or keys |

**Runtime**

- `RUN_LOAD_TYPE`: `full` (**default** if unset), `delta`, or `both` (full phase then delta). Details: [`docs/load-types.md`](docs/load-types.md).
- `RUN_DATABASE`, `RUN_TABLE`: optional filters (see above).

## Run commands (local)

From repo root:

```bash
# All enabled full-load tables (default mode)
python -m src.main

# Full + delta in one process
RUN_LOAD_TYPE=both python -m src.main

# Only delta tables
RUN_LOAD_TYPE=delta python -m src.main

# One database alias (all its tables in scope for that mode)
RUN_DATABASE=rfq_service python -m src.main

# One source table (optionally pin database)
RUN_TABLE=recommended_supplier python -m src.main
RUN_DATABASE=rfq_service RUN_TABLE=recommended_supplier python -m src.main
```

## Configuration layout

| Path | Role |
|------|------|
| `config/settings.yaml` | Pipeline name, dataset, S3 layouts, snapshot date mode, extract defaults |
| `config/databases.yaml` | Alias → env key for real Postgres database name |
| `config/tables/full_load/<alias>.yaml` | Full-load table definitions |
| `config/tables/delta_load/<alias>.yaml` | Delta tables (`cursor_column`, etc.) |

## Output layout

Parquet paths follow `destination.full_load_layout` / `delta_load_layout` in `settings.yaml` (placeholders include `table_name`, `snapshot_date`, `load_id`, …). Full loads can delete that table’s snapshot prefix for the date when enabled in settings.

Do not commit secrets; keep `.env` local.
