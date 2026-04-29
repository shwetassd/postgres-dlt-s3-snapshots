# postgres_dlt_s3

Extract tables from **Postgres** and write snapshots to **S3** (or any `dlt` filesystem-compatible bucket URL) using `dlt` with Parquet output.

This project supports:
- **Full load snapshots** (rebuild table snapshot for a given snapshot date)
- **Delta load snapshots** (incremental/merge snapshots based on a cursor column)
- **Parallel execution** across many tables
- **Target filtering** (run a single database/table, or only full/delta)

## Project layout

- `src/main.py`: entrypoint that reads config + runs full/delta phases
- `config/settings.yaml`: pipeline + destination + execution settings
- `config/databases.yaml`: maps database aliases to env var names (actual DB names)
- `config/tables/full_load/*.yaml`: full-load table lists per database alias
- `config/tables/delta_load/*.yaml`: delta-load table lists per database alias

## Prerequisites

- Python 3.10+ recommended
- Network access to your Postgres instance
- AWS credentials configured (if writing to S3)

## Setup (local)

Create and activate a virtual environment, then install deps:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration

### 1) Environment variables

The same variable names must be set in the process environment: locally you can use a `.env` file via `python-dotenv` (`load_dotenv()` is called at startup). On **AWS Batch**, inject Postgres secrets from **Secrets Manager** and non-secret values (host, bucket URL, `POSTGRES_DB_*`, etc.) in the job definition environment, so no `.env` file is required in the container.

**Postgres connection (required)**
- `PG_HOST`
- `PG_PORT` (default: `5432`)
- `PG_USER`
- `PG_PASSWORD`

**Database name mapping (required)**

`config/databases.yaml` defines database aliases and the env var key that stores the *actual* database name.

Example keys you may need in `.env` (depends on what’s in `config/databases.yaml`):
- `POSTGRES_DB_SUPPLIER_STATISTICS=supplier_statistics`
- `POSTGRES_DB_RFQ_BACKEND=rfq_backend`
- `POSTGRES_DB_USER_KRATOS=user-kratos`

**Destination bucket (required)**

`config/settings.yaml` points at `DESTINATION__FILESYSTEM__BUCKET_URL`, so you must set:
- `DESTINATION__FILESYSTEM__BUCKET_URL` (example: `s3://my-bucket-name`)

**AWS credentials (required for S3)**

Provide credentials via your normal AWS setup (environment variables, AWS profile, or IAM role). Common env vars:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` (if using temporary creds)
- `AWS_DEFAULT_REGION` (or `AWS_REGION`)

**Optional runtime controls**
- `DLT_PIPELINES_DIR` (default in this repo: `.dlt_work`)
- `MAX_WORKERS_FULL` (default: `8`)
- `MAX_WORKERS_DELTA` (default: `6`)

**Optional run filters**
- `RUN_DATABASE` (database alias from `config/databases.yaml`, e.g. `statistics`)
- `RUN_TABLE` (table name only, e.g. `orders`)
- `RUN_LOAD_TYPE` (`full` or `delta`)

### 2) YAML settings

Edit `config/settings.yaml` to control:
- **Run mode**: `execution.run_mode` ∈ `full_then_delta | full_only | delta_only`
- **Snapshot date**: currently `snapshot.mode: current_date` and `snapshot.date_format: "%Y/%m/%d"`
- **Layouts**: `destination.full_load_layout`, `destination.delta_load_layout`
- **Delete existing snapshot**: `snapshot.delete_existing_full_load_snapshot`

### 3) Table selection

Tables are listed per database alias in:
- `config/tables/full_load/<db_alias>.yaml`
- `config/tables/delta_load/<db_alias>.yaml`

Each file contains a `tables:` list with entries like `schema`, `table`, and (for delta) `primary_key`, `cursor_column`, `initial_value`, etc.

## Run

Run from the repo root.

### Run using your `.env` file

```bash
source .venv/bin/activate
python -m src.main
```

### Run only FULL or only DELTA (without editing YAML)

```bash
# only full loads
RUN_LOAD_TYPE=full python -m src.main

# only delta loads
RUN_LOAD_TYPE=delta python -m src.main
```

### Run a single database alias

```bash
RUN_DATABASE=statistics python -m src.main
```

### Run a single table (across selected database / configs)

```bash
RUN_TABLE=some_table_name python -m src.main
```

### Combine filters

```bash
RUN_DATABASE=statistics RUN_TABLE=some_table_name RUN_LOAD_TYPE=full python -m src.main
```

## Output

By default, snapshots are written to the bucket URL under:
- `full_load/<table_name>/<snapshot_date>/...` for full loads
- `delta/<table_name>/<snapshot_date>/...` for delta loads

The exact object key layout is controlled by `config/settings.yaml` (`destination.*_layout`).

## Notes / troubleshooting

- Never commit real credentials in `.env`. Prefer keeping `.env` local-only and (optionally) maintaining a sanitized `.env.example` for documentation.
- If you see errors about missing DB env vars, ensure the env var referenced by `config/databases.yaml` is set (e.g. `POSTGRES_DB_*`).
- If S3 writes fail, verify AWS credentials and that `DESTINATION__FILESYSTEM__BUCKET_URL` points to a bucket you can write to.
- If imports fail, make sure you’re running from the repo root and using `python -m src.main`.
