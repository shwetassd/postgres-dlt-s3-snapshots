import os
from pathlib import Path


def get_pipelines_dir(settings: dict | None = None) -> str:
    """DLT_PIPELINES_DIR env wins; else ``settings.runtime.dlt_pipelines_dir``; else ``.dlt_work``."""
    env_dir = os.getenv("DLT_PIPELINES_DIR")
    if env_dir is not None and str(env_dir).strip() != "":
        pipelines_dir = env_dir.strip()
    elif settings is not None:
        rt = settings.get("runtime") or {}
        yaml_dir = rt.get("dlt_pipelines_dir")
        pipelines_dir = (
            str(yaml_dir).strip()
            if yaml_dir is not None and str(yaml_dir).strip() != ""
            else ".dlt_work"
        )
    else:
        try:
            from src.config_loader.loader import load_settings

            return get_pipelines_dir(load_settings())
        except Exception:
            pipelines_dir = ".dlt_work"
    Path(pipelines_dir).mkdir(parents=True, exist_ok=True)
    return pipelines_dir
