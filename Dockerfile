FROM ghcr.io/astral-sh/uv:0.12.21 AS uv
FROM python:3.14.10-alpine3.24 AS runtime
RUN apk upgrade --no-cache \
    && python -m pip uninstall -y pip \
    && adduser -D -u 10001 app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app

FROM runtime AS builder
COPY --from=uv /uv /uvx /bin/
ENV UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-cache

FROM runtime AS base
COPY --from=builder /app/.venv ./.venv
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations
COPY examples ./examples
USER app
CMD ["uvicorn", "mkk_bisness_test.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

FROM builder AS test
RUN uv sync --frozen --no-cache
COPY alembic.ini ./
COPY migrations ./migrations
COPY tests ./tests
USER app
CMD ["pytest", "-q", "-p", "no:cacheprovider"]

FROM postgres:18.6-alpine3.24 AS database
# Startup already runs as postgres; the root-only privilege helper is unused.
RUN rm /usr/local/bin/gosu
USER postgres

FROM rabbitmq:4.4.0-management AS broker
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
