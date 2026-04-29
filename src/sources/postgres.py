import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


def build_connection_string(database_name: str) -> str:
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    if not all([host, port, user, password, database_name]):
        raise ValueError("Missing one or more required Postgres connection values.")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_name}"


def get_engine(database_name: str):
    conn_str = build_connection_string(database_name)
    # Optional: cancel SQL that runs longer than N ms so workers raise instead of blocking
    # forever (otherwise FULL phase never reaches RUN SUMMARY). Default: unset = PG default.
    connect_args: dict = {}
    st_ms = os.getenv("PG_STATEMENT_TIMEOUT_MS", "").strip()
    if st_ms.isdigit() and int(st_ms) > 0:
        connect_args["options"] = f"-c statement_timeout={int(st_ms)}"
    kw: dict = dict(
        pool_pre_ping=True,
        pool_size=12,
        max_overflow=12,
        pool_recycle=1800,
    )
    if connect_args:
        kw["connect_args"] = connect_args
    # Pool sized for parallel threads per DB; each distinct PG database uses its own engine/pool.
    # If many DB aliases hit one RDS, lower pool_size or raise RDS max_connections.
    return create_engine(conn_str, **kw)