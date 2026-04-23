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
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=8,
        max_overflow=8,
        pool_recycle=1800,
    )