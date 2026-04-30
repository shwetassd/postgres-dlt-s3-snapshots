import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv(override=False)


def build_connection_string(database_name: str) -> str:
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    missing: list[str] = []
    if not host:
        missing.append("PG_HOST")
    if not user:
        missing.append("PG_USER")
    if not password:
        missing.append("PG_PASSWORD")
    if not port:
        missing.append("PG_PORT")
    if not database_name:
        missing.append("database name (from env_database_key for this alias, e.g. POSTGRES_DB_STATISTICS)")

    if missing:
        raise ValueError(
            "Missing Postgres connection env: "
            + ", ".join(missing)
            + ". Set them in the environment or in a .env file in the working directory."
        )

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_name}"


def get_engine(database_name: str):
    conn_str = build_connection_string(database_name)
    # Optional: cancel SQL that runs longer than N ms so workers raise instead of blocking
    # forever (otherwise FULL phase never reaches RUN SUMMARY). Default: unset = PG default.
    connect_args: dict = {}
    st_ms = os.getenv("PG_STATEMENT_TIMEOUT_MS", "").strip()
    if st_ms.isdigit() and int(st_ms) > 0:
        connect_args["options"] = f"-c statement_timeout={int(st_ms)}"
    # Long streamed extracts (pandas chunks): TCP keepalives help with LB/proxy/NAT dropping idle TLS.
    if os.getenv("PG_TCP_KEEPALIVES", "1").lower() in ("1", "true", "yes"):
        connect_args.setdefault("keepalives", 1)
        connect_args.setdefault(
            "keepalives_idle",
            max(1, int(os.getenv("PG_KEEPALIVES_IDLE", "30"))),
        )
        connect_args.setdefault(
            "keepalives_interval",
            max(1, int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))),
        )
        connect_args.setdefault(
            "keepalives_count",
            max(1, int(os.getenv("PG_KEEPALIVES_COUNT", "3"))),
        )
    pool_size = int(os.getenv("SQLALCHEMY_POOL_SIZE", "16"))
    max_overflow = int(os.getenv("SQLALCHEMY_MAX_OVERFLOW", "16"))
    kw: dict = dict(
        pool_pre_ping=True,
        pool_size=max(2, pool_size),
        max_overflow=max(0, max_overflow),
        pool_recycle=1800,
    )
    if connect_args:
        kw["connect_args"] = connect_args
    # Pool sized for parallel threads per DB; each distinct PG database uses its own engine/pool.
    # If many DB aliases hit one RDS, lower pool_size or raise RDS max_connections.
    return create_engine(conn_str, **kw)