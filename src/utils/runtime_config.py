import os
from datetime import date


def get_bucket_url(settings: dict) -> str:
    env_key = settings["destination"]["bucket_url_env"]
    value = os.getenv(env_key)
    if not value:
        raise ValueError(f"{env_key} is not set")
    return value


def get_snapshot_date(settings: dict) -> str:
    snapshot_cfg = settings["snapshot"]
    mode = snapshot_cfg["mode"]
    date_format = snapshot_cfg["date_format"]

    if mode == "current_date":
        return date.today().strftime(date_format)

    raise ValueError(f"Unsupported snapshot mode: {mode}")