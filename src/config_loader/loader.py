from pathlib import Path
import yaml

from src.models.table_config import FullLoadTableConfig, DeltaLoadTableConfig


FULL_LOAD_PATH = Path("config/tables/full_load")
DELTA_LOAD_PATH = Path("config/tables/delta_load")


def load_yaml(path: Path | str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_database_config() -> dict:
    return load_yaml("config/databases.yaml")


def load_settings() -> dict:
    return load_yaml("config/settings.yaml")


def load_full_load_tables() -> list[FullLoadTableConfig]:
    all_tables: list[FullLoadTableConfig] = []

    for file in FULL_LOAD_PATH.glob("*.yaml"):
        database_name = file.stem
        data = load_yaml(file)
        tables = data.get("tables", [])

        for table in tables:
            if not table.get("enabled", True):
                continue

            all_tables.append(
                FullLoadTableConfig(
                    database=database_name,
                    schema=table["schema"],
                    table=table["table"],
                    output_table_name=table.get("output_table_name"),
                    columns=table.get("columns"),
                    select_sql=table.get("select_sql"),
                    output_columns=table.get("output_columns"),
                    enabled=table.get("enabled", True),
                )
            )

    return all_tables


def load_delta_load_tables(default_write_disposition: str) -> list[DeltaLoadTableConfig]:
    all_tables: list[DeltaLoadTableConfig] = []

    for file in DELTA_LOAD_PATH.glob("*.yaml"):
        database_name = file.stem
        data = load_yaml(file)
        tables = data.get("tables", [])

        for table in tables:
            if not table.get("enabled", True):
                continue

            all_tables.append(
                DeltaLoadTableConfig(
                    database=database_name,
                    schema=table["schema"],
                    table=table["table"],
                    primary_key=table["primary_key"],
                    initial_value=table["initial_value"],
                    cursor_column=table["cursor_column"],
                    updated_column=table["updated_column"],
                    write_disposition=table.get("write_disposition", default_write_disposition),
                    output_table_name=table.get("output_table_name"),
                    columns=table.get("columns"),
                    select_sql=table.get("select_sql"),
                    output_columns=table.get("output_columns"),
                    enabled=table.get("enabled", True),
                )
            )

    return all_tables