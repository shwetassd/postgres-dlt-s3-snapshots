import os
from datetime import date


def get_bucket_url(settings: dict) -> str:
    """Resolve filesystem destination root.

    Order: non-empty env named by ``destination.bucket_url_env`` (Batch secrets / job env),
    then non-empty ``destination.bucket_url`` in YAML (image-baked, non-secret layout root).
    """
    dest = settings.get("destination") or {}
    env_key = (dest.get("bucket_url_env") or "DESTINATION__FILESYSTEM__BUCKET_URL").strip()
    env_val = (os.getenv(env_key) or "").strip()
    if env_val:
        return env_val
    yaml_url = dest.get("bucket_url")
    if yaml_url is not None:
        s = str(yaml_url).strip()
        if s:
            return s
    raise ValueError(
        f"Destination bucket URL not set: define non-empty {env_key} or destination.bucket_url "
        "in config/settings.yaml"
    )


def get_snapshot_date(settings: dict) -> str:
    snapshot_cfg = settings["snapshot"]
    mode = snapshot_cfg["mode"]
    date_format = snapshot_cfg["date_format"]

    if mode == "current_date":
        return date.today().strftime(date_format)

    raise ValueError(f"Unsupported snapshot mode: {mode}")


def get_delta_initial_cursor_value(settings: dict) -> str | None:
    """Default lower bound for delta incremental on first run (dlt ``initial_value``).

    ``DELTA_INITIAL_CURSOR_VALUE`` env (empty string = disable) overrides ``settings.delta.initial_cursor_value``.
    Per-table ``initial_value`` in delta YAML overrides this when set.
    """
    if "DELTA_INITIAL_CURSOR_VALUE" in os.environ:
        raw = os.getenv("DELTA_INITIAL_CURSOR_VALUE", "").strip()
        return raw if raw else None
    delta_cfg = settings.get("delta") or {}
    v = delta_cfg.get("initial_cursor_value")
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None