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
    # When set, overrides global EXTRACT_CHUNK_SIZE / settings extract.chunk_size for this table only.
    extract_chunk_size: Optional[int] = None


@dataclass
class DeltaLoadTableConfig:
    database: str
    schema: str
    table: str
    # Result column used as the incremental cursor (max tracked by dlt between runs).
    cursor_column: str
    # Optional PostgreSQL expression over inner row columns, e.g. GREATEST(created_at, updated_at).
    # When set, select_sql is wrapped and this expression is aliased as cursor_column.
    cursor_expression: Optional[str] = None
    output_table_name: Optional[str] = None
    columns: Optional[list[str]] = None
    select_sql: Optional[str] = None
    output_columns: Optional[list[str]] = None
    enabled: bool = True
    extract_chunk_size: Optional[int] = None
    # Optional floor for the cursor on first incremental state (advanced; omit for a full initial extract).
    initial_value: Optional[str] = None