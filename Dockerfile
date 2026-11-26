# One image for every role: the pipeline's driver, the Spark master and the workers. A Python
# function that Spark ships to an executor runs on the executor's Python, so all of them have to
# be the same Python with the same packages.
FROM python:3.13-slim-trixie AS base

# PySpark 4 runs on Java 17 or 21. nss_wrapper: see docker/entrypoint.sh.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless libnss-wrapper \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.10.2 /uv /usr/local/bin/uv
COPY docker/entrypoint.sh /usr/local/bin/entrypoint

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    HOME=/tmp \
    SPARK_WORKER_DIR=/tmp/spark-worker

WORKDIR /app
ENTRYPOINT ["entrypoint"]

FROM base AS dev
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-install-project
COPY src ./src
RUN uv sync --locked

FROM base AS runtime
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --no-editable
USER 1000:1000
CMD ["wattlake", "--help"]
