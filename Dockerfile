FROM python:3.12-slim AS builder

# --- Install Poetry ---
ARG POETRY_VERSION=2.1

ENV POETRY_HOME=/opt/poetry
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV PYTHON_PACKAGE=fishsense_data_processing_spider
# Tell Poetry where to place its cache and virtual environment
ENV POETRY_CACHE_DIR=/opt/.cache

RUN pip install "poetry==${POETRY_VERSION}"

WORKDIR /app

# --- Reproduce the environment ---
# You can comment the following two lines if you prefer to manually install
#   the dependencies from inside the container.
COPY pyproject.toml poetry.lock /app/

# Install the dependencies and clear the cache afterwards.
#   This may save some MBs.
RUN poetry install --no-root --without dev && rm -rf $POETRY_CACHE_DIR

COPY README.md /app/README.md
COPY ${PYTHON_PACKAGE} /app/${PYTHON_PACKAGE}
RUN poetry install --only main

# Now let's build the runtime image from the builder.
#   We'll just copy the env and the PATH reference.
FROM python:3.12-slim AS runtime
# Install exiftool
ADD --checksum=sha256:1cd555144846a28298783bebf3ab452235273c78358410813e3d4e93c653b1fa https://exiftool.org/Image-ExifTool-13.25.tar.gz /tmp
RUN tar -xzvf /tmp/Image-ExifTool-13.25.tar.gz
ENV E4EFS_EXIFTOOL__PATH=/Image-ExifTool-13.25/exiftool

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV E4EFS_DOCKER=true

RUN mkdir -p /e4efs/config /e4efs/logs /e4efs/data /e4efs/cache
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/${PYTHON_PACKAGE} /app/${PYTHON_PACKAGE}
COPY sql sql


ENTRYPOINT ["fsl_spider"]
