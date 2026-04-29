import logging

import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_database, remove_nullability_adapter
from sqlalchemy import MetaData, Table, select, text

from src.utils.load_metrics import normalize_row_counts
from src.utils.workdir_cleanup import log_hints_after_no_space
from src.utils.sql_dataframe import normalize_sql_chunk_dtypes

log = logging.getLogger(__name__)


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


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
    columns: list[str] | None,
    select_sql: str | None,
    output_columns: list[str] | None,
    primary_key: str,
    cursor_column: str,
    initial_value: str,
    write_disposition: str,
    snapshot_date: str,
    extract_chunk_size: int = 100000,
    extract_backend: str = "pyarrow",
) -> tuple[object, dict[str, int]]:
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

    if select_sql:
        import pandas as pd

        base_query = (
            f"SELECT {select_sql} "
            f"FROM {quote_ident(schema_name)}.{quote_ident(source_table_name)}"
        )

        @dlt.resource(
            name=table_name,
            primary_key=primary_key,
            write_disposition=write_disposition,
        )
        def projected_rows(
            incremental=dlt.sources.incremental(cursor_column, initial_value=initial_value),
        ):
            last_value = incremental.last_value
            query_text = base_query
            params = None

            if last_value is not None:
                query_text = (
                    f"SELECT * FROM ({base_query}) AS src "
                    f"WHERE {quote_ident(cursor_column)} > :last_value"
                )
                params = {"last_value": last_value}

            with engine.connect() as raw_conn:
                conn = raw_conn.execution_options(stream_results=True)
                for chunk_df in pd.read_sql_query(
                    sql=text(query_text),
                    con=conn,
                    params=params,
                    chunksize=extract_chunk_size,
                ):
                    chunk_df = normalize_sql_chunk_dtypes(chunk_df)
                    yield chunk_df

        try:
            load_info = pipeline.run(
                projected_rows(),
                table_name=table_name,
                loader_file_format=file_format,
            )
        except Exception as e:
            log.exception(
                "DELTA load failed schema=%s source_table=%s output_table=%s cursor=%s",
                schema_name,
                source_table_name,
                table_name,
                cursor_column,
            )
            raise RuntimeError(
                f"{schema_name}.{source_table_name} (output={table_name}, cursor={cursor_column}): {e}"
            ) from e
        return load_info, normalize_row_counts(pipeline)

    def table_adapter_callback(table):
        table = remove_nullability_adapter(table)

        if output_columns:
            for col in list(table._columns):
                if col.name not in output_columns:
                    table._columns.remove(col)

        return table

    source = sql_database(
        engine,
        schema=schema_name,
        table_names=[source_table_name],
        chunk_size=extract_chunk_size,
        backend=extract_backend,
        reflection_level="minimal",
        table_adapter_callback=table_adapter_callback,
    )

    resource = source.with_resources(source_table_name).resources[source_table_name]

    if columns:
        metadata = MetaData(schema=schema_name)
        table = Table(source_table_name, metadata, autoload_with=engine)
        selected_columns = [table.c[column] for column in columns]
        projected_query = select(*selected_columns)
        resource.query_adapter_callback = (
            lambda query_obj, table_obj, incremental=None, engine_obj=None: projected_query
        )

    resource.apply_hints(
        table_name=table_name,
        write_disposition=write_disposition,
        primary_key=primary_key,
        incremental=dlt.sources.incremental(
            cursor_path=cursor_column,
            initial_value=initial_value,
        ),
    )

    try:
        load_info = pipeline.run(
            resource,
            loader_file_format=file_format,
        )
    except Exception as e:
        log_hints_after_no_space(log, e)
        log.exception(
            "DELTA load failed schema=%s source_table=%s output_table=%s cursor=%s",
            schema_name,
            source_table_name,
            table_name,
            cursor_column,
        )
        raise RuntimeError(
            f"{schema_name}.{source_table_name} (output={table_name}, cursor={cursor_column}): {e}"
        ) from e
    return load_info, normalize_row_counts(pipeline)