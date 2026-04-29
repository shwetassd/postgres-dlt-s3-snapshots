from dataclasses import dataclass
from typing import Optional


@dataclass
class FullLoadTableConfig:
    database: str
    schema: str
    table: str
    output_table_name: Optional[str] = None
    columns: Optional[list[str]] = None
    select_sql: Optional[str] = None
    output_columns: Optional[list[str]] = None
    enabled: bool = True


@dataclass
class DeltaLoadTableConfig:
    database: str
    schema: str
    table: str
    primary_key: str
    initial_value: str
    cursor_column: str
    updated_column: str
    write_disposition: str = "merge"
    output_table_name: Optional[str] = None
    columns: Optional[list[str]] = None
    select_sql: Optional[str] = None
    output_columns: Optional[list[str]] = None
    enabled: bool = True