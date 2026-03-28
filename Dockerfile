# ---- build stage: install deps and run tests ----
FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source for test run
COPY . .

# Ensure data dir exists for tests that need it
RUN mkdir -p data

# Run tests; fail the build if any test fails
RUN python -m pytest tests/ -v --tb=short

# ---- runtime stage ----
FROM python:3.12-slim AS runtime

WORKDIR /app

# Install only runtime deps (no pytest/httpx)
COPY requirements.txt .
RUN pip install --no-cache-dir \
    openai \
    duckdb \
    fastapi \
    uvicorn \
    python-dotenv \
    prometheus_client

# Copy application source (tests excluded via .dockerignore)
COPY agent/       agent/
COPY api/         api/
COPY lineage/     lineage/
COPY observability/ observability/
COPY pipeline/    pipeline/
COPY ui/          ui/

# The data/ directory is mounted as a PersistentVolume at runtime.
# Create it so the app can write before the volume is mounted in dev.
RUN mkdir -p data

EXPOSE 3000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "3000"]
