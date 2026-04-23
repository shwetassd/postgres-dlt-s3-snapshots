def sanitize_name(value: str) -> str:
    return value.replace("-", "_").replace(".", "_").strip("_")


def build_pipeline_name(
    base_name: str,
    load_type: str,
    database: str,
    schema: str,
    table: str,
) -> str:
    return "__".join(
        [
            sanitize_name(base_name),
            sanitize_name(load_type.lower()),
            sanitize_name(database),
            sanitize_name(schema),
            sanitize_name(table),
        ]
    )