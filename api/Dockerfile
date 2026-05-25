# ─────────────────────────────────────────────────────────────────────────────
# PulseTrade — FastAPI agent image (Option A: MCP server co-located)
#
# WHY THIS SHAPE:
#   The agent (LangGraph + Gemini) runs in this process. When an investigation
#   starts, agent/mcp_client.py spawns `python -m mcp_server.server` as a stdio
#   subprocess (cwd = this WORKDIR), uses its three tools, and tears it down at
#   the end. Both the agent code and the MCP server code therefore live in the
#   SAME image — that is the whole point of Option A. No transport change, no
#   second service. See docs/adr/ADR-008 for the A-vs-B reasoning.
#
#   Multi-stage: a fat "builder" compiles wheels (some deps — grpcio, pyarrow,
#   psycopg2 — need build toolchains), then we copy ONLY the installed packages
#   into a clean slim runtime. Final image carries no compilers = smaller and
#   smaller attack surface.
# ─────────────────────────────────────────────────────────────────────────────

# ---- Stage 1: builder ----------------------------------------------------------
FROM python:3.11-slim AS builder

# Build-time system deps. gcc/g++ for any package without a manylinux wheel;
# libpq-dev so psycopg2-binary's headers resolve if a source build is triggered.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install into an isolated prefix we can copy wholesale into the runtime stage.
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.txt .

# Build wheels into /install. --prefix keeps everything under one tree so the
# runtime stage gets site-packages + console scripts in a single COPY.
RUN pip install --prefix=/install -r requirements.txt

# ---- Stage 2: runtime ----------------------------------------------------------
FROM python:3.11-slim AS runtime

# Runtime-only system deps. libpq5 is the shared lib psycopg2 needs at run time
# (NOT libpq-dev — we don't compile here). No gcc in the final image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# WHY a non-root user: if the container is ever compromised, the blast radius is
# a low-privilege account, not root. K8s security contexts often REQUIRE this.
RUN useradd --create-home --uid 10001 appuser

# Pull the built packages from the builder stage. /install/* maps onto /usr/local
# (python:3.11-slim's prefix), so site-packages and entrypoints land where the
# interpreter already looks.
COPY --from=builder /install /usr/local

# WORKDIR = /app. This MUST match what mcp_client.py expects as cwd when it spawns
# `python -m mcp_server.server`. Because we copy the whole api/ tree here and run
# from here, `-m mcp_server.server` resolves exactly as it does on your laptop.
WORKDIR /app

# Copy application code. .dockerignore keeps .venv, __pycache__, .env, tests out.
COPY . .

# Drop to non-root for everything that follows.
RUN chown -R appuser:appuser /app
USER appuser

# Sensible Python container defaults:
#   PYTHONUNBUFFERED      — logs flush immediately (critical: your MCP server
#                           prints to stderr; buffering would hide those lines)
#   PYTHONDONTWRITEBYTECODE — no .pyc clutter in a read-only-ish container
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

# Single uvicorn worker per container ON PURPOSE. "One MCP subprocess per worker"
# means multiple workers = multiple MCP subprocesses inside one container. We
# scale by adding K8s replicas instead — cleaner, and each replica is one tidy
# (agent + its MCP subprocess) unit. No --reload in prod.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
