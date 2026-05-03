import os

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from src.utils.dotenv_policy import load_dotenv_optional


load_dotenv_optional()


def _postgres_yaml_defaults() -> dict:
    try:
        from src.config_loader.loader import load_settings

        return load_settings().get("postgres") or {}
    except Exception:
        return {}


def build_connection_string(database_name: str) -> str:
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    port_int = _parse_pg_port(os.getenv("PG_PORT"))

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
            + ". Set them in the job environment (e.g. AWS Batch secrets) or use a local .env "
            "when SKIP_DOTENV is unset."
        )

    query: dict[str, str] = {}
    sslmode = os.getenv("PG_SSLMODE", "").strip()
    if sslmode:
        query["sslmode"] = sslmode

    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=database_name,
        query=query,
    )
    return url.render_as_string(hide_password=False)


def get_engine(database_name: str):
    conn_str = build_connection_string(database_name)
    pg = _postgres_yaml_defaults()
    # Optional: cancel SQL that runs longer than N ms so workers raise instead of blocking
    # forever (otherwise FULL phase never reaches RUN SUMMARY). Default: unset = PG default.
    connect_args: dict = {}
    st_ms = os.getenv("PG_STATEMENT_TIMEOUT_MS", "").strip()
    if not st_ms and pg.get("statement_timeout_ms") is not None:
        st_ms = str(pg["statement_timeout_ms"]).strip()
    if st_ms.isdigit() and int(st_ms) > 0:
        connect_args["options"] = f"-c statement_timeout={int(st_ms)}"
    # Long streamed extracts (pandas chunks): TCP keepalives help with LB/proxy/NAT dropping idle TLS.
    def _truthy_env(env_k: str, yaml_k: str, default: bool) -> bool:
        raw = os.getenv(env_k)
        if raw is not None and raw.strip() != "":
            return raw.lower() in ("1", "true", "yes")
        if yaml_k in pg and pg[yaml_k] is not None:
            return bool(pg[yaml_k])
        return default

    def _int_env(env_k: str, yaml_k: str, default: int) -> int:
        raw = os.getenv(env_k)
        if raw is not None and raw.strip() != "":
            return int(raw)
        if yaml_k in pg and pg[yaml_k] is not None:
            return int(pg[yaml_k])
        return default

    if _truthy_env("PG_TCP_KEEPALIVES", "tcp_keepalives_enabled", True):
        connect_args.setdefault("keepalives", 1)
        connect_args.setdefault(
            "keepalives_idle",
            max(1, _int_env("PG_KEEPALIVES_IDLE", "keepalives_idle", 30)),
        )
        connect_args.setdefault(
            "keepalives_interval",
            max(1, _int_env("PG_KEEPALIVES_INTERVAL", "keepalives_interval", 10)),
        )
        connect_args.setdefault(
            "keepalives_count",
            max(1, _int_env("PG_KEEPALIVES_COUNT", "keepalives_count", 3)),
        )
    pool_size = _int_env("SQLALCHEMY_POOL_SIZE", "sqlalchemy_pool_size", 16)
    max_overflow = _int_env("SQLALCHEMY_MAX_OVERFLOW", "sqlalchemy_max_overflow", 16)
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