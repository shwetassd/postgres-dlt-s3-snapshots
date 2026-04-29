"""Detect transient failures worth retrying (S3 upload / network), not logic bugs."""

from __future__ import annotations

try:
    from botocore.exceptions import ClientError as _BotoClientError
except ImportError:
    _BotoClientError = None


def _walk_exception_chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    seen: set[int] = set()
    e: BaseException | None = exc
    while e is not None and id(e) not in seen:
        seen.add(id(e))
        out.append(e)
        e = e.__cause__ or e.__context__
    return out


def is_retryable_full_load_error(exc: BaseException) -> bool:
    """True for likely-transient S3 / HTTP / network errors during extract→load."""
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
            # EPIPE, ECONNRESET, ETIMEDOUT; errno 22 often used for S3 Content-Length mismatch (Linux EINVAL)
            errno = getattr(err, "errno", None)
            if errno in (32, 104, 110):
                return True
            if errno == 22:
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
                "timed out",
                "timeout",
                "slowdown",
                "throttl",
                "please reduce your request rate",
            )
        ):
            return True

    return False
