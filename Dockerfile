FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps:
# - libpq5: runtime dependency for psycopg2
# - build-essential + libpq-dev: may be needed if wheels aren't available for the platform
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    libpq5 \
    libpq-dev \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip \
  && pip install -r /app/requirements.txt

COPY . /app

# Run as non-root (recommended for AWS Batch and general container hardening)
RUN useradd -m -u 10001 appuser \
  && chown -R appuser:appuser /app
USER appuser

# Default entrypoint for AWS Batch; override CMD/env in Job Definition as needed.
CMD ["python", "-m", "src.main"]
