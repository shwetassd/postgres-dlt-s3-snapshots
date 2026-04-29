"""Local disk hygiene for dlt pipeline working dirs (default ``DLT_PIPELINES_DIR`` / ``.dlt_work``)."""

from __future__ import annotations

import errno
import gc
import logging
import shutil
from pathlib import Path


def clear_pipeline_workdir(pipelines_dir: str, *, logger: logging.Logger | None = None) -> None:
    """Delete all contents under ``pipelines_dir``. Safe **before** any ``pipeline.run`` for this job.

    Use when replaying a failed host-mounted volume or shrinking leftovers from a prior run.
    Fresh ECS/Batch tasks usually start with an empty filesystem — this mainly helps dev reruns
    or tasks that reuse a bind mount.
    """
    log = logger or logging.getLogger(__name__)
    root = Path(pipelines_dir).resolve()
    if not root.exists():
        return
    removed = 0
    for child in list(root.iterdir()):
        try:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            removed += 1
        except OSError as ex:
            log.warning("Could not remove %s: %s", child, ex)
    if removed:
        log.info(
            "PIPELINE_CLEAR_WORKDIR_BEFORE_RUN: removed %s path(s) under %s",
            removed,
            root,
        )
    else:
        log.info("PIPELINE_CLEAR_WORKDIR_BEFORE_RUN: nothing to remove under %s", root)


def exception_errno(exc: BaseException | None) -> int | None:
    """Walk ``__cause__`` / ``__context__`` for an ``OSError`` errno (e.g. ENOSPC)."""
    if exc is None:
        return None
    if isinstance(exc, OSError) and exc.errno is not None:
        return exc.errno
    r = exception_errno(exc.__cause__)
    if r is not None:
        return r
    return exception_errno(exc.__context__)


def log_hints_after_no_space(logger: logging.Logger, exc: BaseException) -> None:
    """If ``exc`` is (or wraps) ENOSPC: run ``gc.collect()`` and log sizing guidance.

    Does **not** delete other pipelines' folders during parallel loads — that would corrupt runs.
    """
    if exception_errno(exc) != errno.ENOSPC:
        return
    gc.collect()
    logger.warning(
        "Disk full (ENOSPC): gc.collect() ran; space rarely appears mid-run without failing again. "
        "Increase task ephemeral storage, lower MAX_WORKERS_* / EXTRACT_CHUNK_SIZE, or enable "
        "PIPELINE_CLEAR_WORKDIR_BEFORE_RUN=1 on the **next** run. "
        "Memory pressure / OOM is separate: reduce chunk size and workers — Python cannot reclaim RAM held by busy threads.",
    )
