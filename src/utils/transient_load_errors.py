"""Retry policy: only infra/transient errors (S3, network, memory, disk); never schema/config bugs."""

from __future__ import annotations

import errno

try:
    from botocore.exceptions import ClientError as _BotoClientError
except ImportError:
    _BotoClientError = None

# Failures that must not burn retries (YAML/schema/dlt logic — fix config instead).
_SCHEMA_OR_LOGIC_MARKERS = (
    "table schema does not match",
    "schema does not match schema used to create file",
    "extract/schema mismatch",
    "merging arrow schema",
    "column hints were different",
    "undefinedcolumn",
    "syntax error at",
    "programmingerror",
    "duplicate key value violates unique constraint",
)


def _walk_exception_chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    seen: set[int] = set()
    e: BaseException | None = exc
    while e is not None and id(e) not in seen:
        seen.add(id(e))
        out.append(e)
        e = e.__cause__ or e.__context__
    return out


def is_schema_or_logic_failure(exc: BaseException) -> bool:
    """True when failure should never be retried (schema, SQL, dlt contract)."""
    for err in _walk_exception_chain(exc):
        msg = str(err).lower()
        if isinstance(err, ValueError) and "schema" in msg and "match" in msg:
            return True
        for sub in _SCHEMA_OR_LOGIC_MARKERS:
            if sub in msg:
                return True
    return False


def is_retryable_full_load_error(exc: BaseException) -> bool:
    """True only for infra/transient issues worth retrying (OOM, disk, S3/network flakiness)."""
    if is_schema_or_logic_failure(exc):
        return False

    for err in _walk_exception_chain(exc):
        if _BotoClientError is not None and isinstance(err, _BotoClientError):
            code = (
                (err.response or {})
                .get("Error", {})
                .get("Code", "")
            )
            if code in (
                "Throttling",
                "SlowDown",
                "RequestTimeout",
                "ServiceUnavailable",
                "InternalError",
                "EC2ThrottledException",
            ):
                return True

        if isinstance(err, (BrokenPipeError, TimeoutError)):
            return True

        if isinstance(err, OSError):
            no = getattr(err, "errno", None)
            # ENOMEM, ENOSPC, EPIPE, ECONNRESET, ETIMEDOUT (values vary by OS; 28 is ENOSPC on Linux/macOS)
            if no in (
                getattr(errno, "ENOMEM", 12),
                getattr(errno, "ENOSPC", 28),
                32,
                104,
                110,
            ):
                return True
            if no == 22:
                msg = str(err).lower()
                if "content-length" in msg or "number of bytes" in msg:
                    return True

        msg = str(err).lower()
        if any(
            n in msg
            for n in (
                "content-length",
                "number of bytes specified",
                "connection lost",
                "broken pipe",
                "connection reset",
                "reset by peer",
                "cannot allocate memory",
                "no space left",
                "timed out",
                "timeout",
                "slowdown",
                "throttl",
                "please reduce your request rate",
                # PostgreSQL / psycopg2 mid-query disconnect (RDS idle timeout, proxy, network blip)
                "ssl syscall",
                "eof detected",
                "could not receive data from server",
                "server closed the connection unexpectedly",
            )
        ):
            return True

    return False


def is_memory_or_disk_exhaustion(exc: BaseException) -> bool:
    """OOM or ENOSPC — optional aggressive local cleanup before retry."""
    for err in _walk_exception_chain(exc):
        if isinstance(err, MemoryError):
            return True
        if isinstance(err, OSError) and getattr(err, "errno", None) == getattr(errno, "ENOSPC", 28):
            return True
    return False
