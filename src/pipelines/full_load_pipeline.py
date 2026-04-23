import dlt
import pandas as pd
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_database, remove_nullability_adapter
from sqlalchemy import MetaData, Table, select, text


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def run_full_snapshot(
    engine,
    pipeline_name: str,
    pipelines_dir: str,
    dataset_name: str,
    bucket_url: str,
    layout: str,
    file_format: str,
    schema_name: str,
    table_name: str,
    columns: list[str] | None,
    select_sql: str | None,
    output_columns: list[str] | None,
    snapshot_date: str,
    extract_chunk_size: int = 100000,
    extract_backend: str = "pyarrow",
):
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

    # Fast path for custom SQL projections:
    # yield pandas DataFrames instead of Python dict rows
    if select_sql:
        projected_query = (
            f"SELECT {select_sql} "
            f"FROM {quote_ident(schema_name)}.{quote_ident(table_name)}"
        )

        @dlt.resource(name=table_name, write_disposition="append")
        def projected_rows():
            for chunk_df in pd.read_sql_query(
                sql=text(projected_query),
                con=engine,
                chunksize=extract_chunk_size,
            ):
                yield chunk_df

        return pipeline.run(
            projected_rows(),
            table_name=table_name,
            loader_file_format=file_format,
        )

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
        table_names=[table_name],
        chunk_size=extract_chunk_size,
        backend=extract_backend,
        reflection_level="minimal",
        table_adapter_callback=table_adapter_callback,
    )

    resource = source.with_resources(table_name).resources[table_name]

    if columns:
        metadata = MetaData(schema=schema_name)
        table = Table(table_name, metadata, autoload_with=engine)
        selected_columns = [table.c[column] for column in columns]
        projected_query = select(*selected_columns)
        resource.query_adapter_callback = (
            lambda query_obj, table_obj, incremental=None, engine_obj=None: projected_query
        )

    resource.apply_hints(write_disposition="append")

    return pipeline.run(
        resource,
        table_name=table_name,
        loader_file_format=file_format,
    )