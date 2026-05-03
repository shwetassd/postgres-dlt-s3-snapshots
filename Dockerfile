# Builder: install Python deps with compilers available if any wheel is missing.
FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip \
  && python -m venv /opt/venv \
  && /opt/venv/bin/pip install -r /app/requirements.txt

# Runtime: minimal OS libs + venv only (smaller image for AWS Batch).
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/opt/venv/bin:${PATH}" \
    TMPDIR=/tmp \
    # Do not read `.env` in the container; use Batch / ECS environment and secrets only.
    SKIP_DOTENV=1

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    libpq5 \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
COPY . /app

RUN useradd -m -u 10001 appuser \
  && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "src.main"]
