# ─────────────────────────────────────────────────────────────────────────────
# PulseTrade — Streamlit dashboard image
#
# WHY THIS SHAPE:
#   The dashboard is a read-mostly UI. Per the Day 7 "reads bypass FastAPI"
#   split, it queries Databricks/Postgres DIRECTLY for display, and only the
#   Trigger panel calls FastAPI /investigate over HTTP. So this image needs the
#   DB drivers (databricks-sql-connector, psycopg2) but does NOT contain any
#   agent or MCP code — it's a separate deployable, scaled independently (Day 7
#   established it as its own service with its own venv).
#
#   Same multi-stage + non-root pattern as the api image, for the same reasons.
# ─────────────────────────────────────────────────────────────────────────────

# ---- Stage 1: builder ----------------------------------------------------------
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# ---- Stage 2: runtime ----------------------------------------------------------
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 appuser

COPY --from=builder /install /usr/local

WORKDIR /app
COPY . .

RUN chown -R appuser:appuser /app
USER appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Streamlit's default port.
EXPOSE 8501

# WHY these flags:
#   --server.address=0.0.0.0  — bind all interfaces so it's reachable outside the
#                               container (default 'localhost' would be unreachable)
#   --server.port=8501        — explicit, matches EXPOSE and the future K8s Service
#   headless + gatherUsageStats=false — no browser auto-open, no telemetry phone-home
CMD ["streamlit", "run", "app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
