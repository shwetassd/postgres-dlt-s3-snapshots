import logging
import time
from datetime import date, datetime

import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import remove_nullability_adapter, sql_table
from sqlalchemy import text

from src.utils.load_metrics import normalize_row_counts
from src.utils.sql_dataframe import normalize_sql_chunk_dtypes
from src.utils.workdir_cleanup import log_hints_after_no_space

log = logging.getLogger(__name__)


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _coerce_dlt_incremental_initial_value(value):
    """Match dlt/PyArrow cursor dtype (e.g. timestamp[us]): strings like ISO dates must be datetime, not str."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) if isinstance(value, float) and value == int(value) else value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
        iso = s[:-1] + "+00:00" if s.endswith("Z") else s
        try:
            return datetime.fromisoformat(iso)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return value
    return value


def _format_watermark_for_log(value) -> tuple[str, str]:
    """Return (human_readable, raw_repr) for incremental cursor / last load high-water mark."""
    if value is None:
        return (
            "none (no prior successful load for this pipeline — full rowset this run)",
            "None",
        )
    readable = None
    try:
        if hasattr(value, "isoformat"):
            readable = value.isoformat()
    except Exception:
        readable = None
    if readable is None:
        readable = str(value)
    return (readable, repr(value))


def _delta_watermark_log_message(cursor_column: str, last_v) -> str:
    human, raw = _format_watermark_for_log(last_v)
    if last_v is None:
        return (
            f"No saved high-water mark for `{cursor_column}` — extracting all rows once; "
            f"after success, the next run uses `{cursor_column}` > max loaded this run."
        )
    return (
        f"Saved high-water mark from last successful load: `{cursor_column}` last_value={human} "
        f"(raw {raw}). This run loads rows with `{cursor_column}` > that value (exclusive)."
    )


def _watermark_summary_line(cursor_column: str, last_v) -> str:
    """Fallback one-line INFO when min/max query failed; details in log.debug."""
    if last_v is None:
        return f"full_table (no saved cursor; column {cursor_column})"
    human, _ = _format_watermark_for_log(last_v)
    return f"incremental (column {cursor_column}; load rows newer than {human})"


def _format_cursor_value_for_log(value) -> str:
    if value is None:
        return "n/a"
    try:
        if hasattr(value, "isoformat"):
            return value.isoformat()
    except Exception:
        pass
    return str(value)


def _cursor_min_max_statement(projected_base: str, cursor_sql_ref: str, cursor_column: str, last_v):
    """Aggregate MIN/MAX of cursor over the same rowset as the delta extract (one DB round-trip)."""
    col = quote_ident(cursor_column)
    inner = projected_base
    if last_v is not None:
        inner = f"{projected_base} WHERE {cursor_sql_ref} > :lv"
    sql = f"SELECT MIN(_dlt_b.{col}), MAX(_dlt_b.{col}) FROM ({inner}) AS _dlt_b"
    stmt = text(sql)
    if last_v is not None:
        stmt = stmt.bindparams(lv=last_v)
    return stmt


def _compact_delta_extract_description(cursor_column: str, last_v, lo, hi) -> str:
    """Single INFO line. lo/hi are MIN/MAX of cursor_column (e.g. GREATEST(created_at,updated_at)), not MIN(created_at)."""
    if lo is None and hi is None:
        if last_v is None:
            return (
                f"full_table column={cursor_column} "
                "no_rows_or_null_watermarks_in_aggregate"
            )
        cutoff, _ = _format_watermark_for_log(last_v)
        return (
            f"incremental column={cursor_column} "
            f"cutoff_exclusive={cutoff} matched_rows=0"
        )

    lo_s = _format_cursor_value_for_log(lo)
    hi_s = _format_cursor_value_for_log(hi)
    if last_v is None:
        return (
            f"full_table column={cursor_column} "
            f"loading_all_rows dlt_cursor_min={lo_s} dlt_cursor_max={hi_s}"
        )

    cutoff, _ = _format_watermark_for_log(last_v)
    return (
        f"incremental column={cursor_column} "
        f"cutoff_exclusive={cutoff} "
        f"dlt_cursor_min={lo_s} dlt_cursor_max={hi_s}"
    )


def run_delta_snapshot(
    engine,
    pipeline_name: str,
    pipelines_dir: str,
    dataset_name: str,
    bucket_url: str,
    layout: str,
    file_format: str,
    schema_name: str,
    source_table_name: str,
    table_name: str,
    cursor_column: str,
    columns: list[str] | None,
    select_sql: str | None,
    output_columns: list[str] | None,
    snapshot_date: str,
    extract_chunk_size: int = 100000,
    extract_backend: str = "pandas",
    initial_value=None,
    cursor_expression: str | None = None,
) -> tuple[object, dict[str, int]]:
    """Incremental extract using cursor_column.

    First vs later runs: dlt persists state under ``pipelines_dir`` for ``pipeline_name`` (not S3 paths).
    First run has no saved cursor → full extract; later runs use ``WHERE cursor > last_saved``.

    If ``cursor_expression`` is set, it is evaluated per row and aliased as ``cursor_column``
    (e.g. ``GREATEST(created_at, updated_at) AS dlt_cursor``). Use ``select_sql`` or omit it and
    supply ``columns`` so the pipeline builds the inner SELECT — either way one watermark covers
    new and updated rows.
    """
    destination = filesystem(
        bucket_url=bucket_url,
        layout=layout,
        extra_placeholders={"snapshot_date": snapshot_date},
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        pipelines_dir=pipelines_dir,
        destination=destination,
        dataset_name=dataset_name,
    )

    coerced_initial = _coerce_dlt_incremental_initial_value(initial_value)
    incr_kw: dict = dict(row_order="asc", range_start="open")
    if coerced_initial is not None:
        incr_kw["initial_value"] = coerced_initial
    incremental_conf = dlt.sources.incremental(cursor_column, **incr_kw)

    # cursor_expression is applied in the SQL projection below. YAML may omit select_sql and list
    # physical columns only — we synthesize the inner SELECT so incremental watermark matches
    # GREATEST(...) (etc.), not a non-existent physical `dlt_cursor` column on the table.
    effective_select_sql = select_sql
    if cursor_expression and not effective_select_sql:
        if not columns:
            raise ValueError(
                f"{quote_ident(schema_name)}.{quote_ident(source_table_name)}: "
                "cursor_expression requires select_sql or a columns list naming physical columns."
            )
        effective_select_sql = ", ".join(quote_ident(c) for c in columns)

    if effective_select_sql:
        import pandas as pd

        # Avoid alias `inner` — it is reserved (INNER JOIN) and breaks PostgreSQL parsing before `inner.*`.
        inner_from = (
            f"(SELECT {effective_select_sql} FROM {quote_ident(schema_name)}.{quote_ident(source_table_name)}) AS dlt_src"
        )
        if cursor_expression:
            projected_base = (
                "SELECT * FROM ("
                "SELECT dlt_src.*, ("
                + cursor_expression.strip()
                + ") AS "
                + quote_ident(cursor_column)
                + " FROM "
                + inner_from
                + ") sub"
            )
            cursor_sql_ref = "sub." + quote_ident(cursor_column)
        else:
            projected_base = "SELECT " + effective_select_sql + " FROM " + quote_ident(schema_name) + "." + quote_ident(source_table_name)
            cursor_sql_ref = quote_ident(cursor_column)

        order_clause = f" ORDER BY {cursor_sql_ref} ASC"

        @dlt.resource(name=table_name, write_disposition="append")
        def projected_delta_rows(incr_bind=incremental_conf):
            last_v = incr_bind.last_value
            bounds_stmt = _cursor_min_max_statement(
                projected_base, cursor_sql_ref, cursor_column, last_v
            )
            bounds_lo = bounds_hi = None
            bounds_ok = False
            try:
                with engine.connect() as conn:
                    row = conn.execute(bounds_stmt).one()
                    bounds_lo, bounds_hi = row[0], row[1]
                    bounds_ok = True
            except Exception as e:
                log.warning(
                    "delta  extract  %s.%s  table_watermark_min_max_query_failed  error=%s",
                    schema_name,
                    source_table_name,
                    e,
                )

            extract_desc = (
                _compact_delta_extract_description(
                    cursor_column, last_v, bounds_lo, bounds_hi
                )
                if bounds_ok
                else _watermark_summary_line(cursor_column, last_v)
            )
            log.info(
                "delta  extract  %s.%s  %s",
                schema_name,
                source_table_name,
                extract_desc,
            )
            log.debug(
                "%s — pipeline=%s table=%s",
                _delta_watermark_log_message(cursor_column, last_v),
                pipeline_name,
                table_name,
            )

            if last_v is None:
                projected_query = text(projected_base + order_clause)
            else:
                projected_query = (
                    text(
                        projected_base
                        + " WHERE "
                        + cursor_sql_ref
                        + " > :lv"
                        + order_clause
                    ).bindparams(lv=last_v)
                )

            log.debug(
                "Delta extract %s.%s (chunksize=%s cursor=%s last_value=%s)",
                schema_name,
                source_table_name,
                extract_chunk_size,
                cursor_column,
                last_v,
            )
            chunk_no = 0
            t0 = time.monotonic()
            with engine.connect() as raw_conn:
                conn = raw_conn.execution_options(stream_results=True)
                read_kw: dict = dict(
                    sql=projected_query,
                    con=conn,
                    chunksize=extract_chunk_size,
                )
                try:
                    major = int(pd.__version__.split(".", 1)[0])
                    if major >= 2:
                        read_kw["dtype_backend"] = "numpy_nullable"
                except (ValueError, TypeError, AttributeError):
                    pass
                log.debug(
                    "delta  waiting for first chunk from PostgreSQL (large tables may take minutes)",
                )
                for chunk_df in pd.read_sql_query(**read_kw):
                    chunk_no += 1
                    rows = len(chunk_df)
                    elapsed = time.monotonic() - t0
                    log.debug(
                        "Chunk %s rows=%s (elapsed=%.1fs)",
                        chunk_no,
                        rows,
                        elapsed,
                    )
                    chunk_df = normalize_sql_chunk_dtypes(chunk_df)
                    yield chunk_df

        try:
            load_info = pipeline.run(
                projected_delta_rows(),
                table_name=table_name,
                loader_file_format=file_format,
            )
        except Exception as e:
            log_hints_after_no_space(log, e)
            log.exception(
                "DELTA load failed schema=%s source_table=%s output_table=%s",
                schema_name,
                source_table_name,
                table_name,
            )
            raise RuntimeError(
                f"{schema_name}.{source_table_name} (output={table_name}): {e}"
            ) from e
        return load_info, normalize_row_counts(pipeline)

    def table_adapter_callback(table):
        table = remove_nullability_adapter(table)

        if output_columns:
            for col in list(table._columns):
                if col.name not in output_columns:
                    table._columns.remove(col)

        return table

    sql_kwargs: dict = dict(
        credentials=engine,
        schema=schema_name,
        table=source_table_name,
        incremental=incremental_conf,
        chunk_size=extract_chunk_size,
        backend=extract_backend,
        reflection_level="minimal",
        table_adapter_callback=table_adapter_callback,
        write_disposition="append",
    )
    if columns:
        sql_kwargs["included_columns"] = columns

    resource = sql_table(**sql_kwargs)

    resource.apply_hints(
        table_name=table_name,
        write_disposition="append",
    )

    sql_wm = getattr(incremental_conf, "last_value", None)
    log.info(
        "delta  sql_table  %s.%s  %s  (no_table_watermark_span_on_this_path)",
        schema_name,
        source_table_name,
        _watermark_summary_line(cursor_column, sql_wm),
    )
    log.debug(
        "delta sql_table pipeline=%s output=%s — %s",
        pipeline_name,
        table_name,
        _delta_watermark_log_message(cursor_column, sql_wm),
    )

    try:
        load_info = pipeline.run(
            resource,
            loader_file_format=file_format,
        )
    except Exception as e:
        log_hints_after_no_space(log, e)
        log.exception(
            "DELTA load failed schema=%s source_table=%s output_table=%s",
            schema_name,
            source_table_name,
            table_name,
        )
        raise RuntimeError(
            f"{schema_name}.{source_table_name} (output={table_name}): {e}"
        ) from e
    return load_info, normalize_row_counts(pipeline)
