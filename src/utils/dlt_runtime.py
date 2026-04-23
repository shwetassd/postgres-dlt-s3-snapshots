import os
from pathlib import Path


def get_pipelines_dir() -> str:
    pipelines_dir = os.getenv("DLT_PIPELINES_DIR", ".dlt_work")
    Path(pipelines_dir).mkdir(parents=True, exist_ok=True)
    return pipelines_dir