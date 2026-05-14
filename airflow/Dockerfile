# Custom Airflow image for PulseTrade.
# Bakes runtime deps into the image so containers start in ~5s instead of
# reinstalling on every boot via _PIP_ADDITIONAL_REQUIREMENTS.
#
# Build:  docker compose build
# Use:    docker compose up -d
#
# Why this exists:
# - pip-on-startup is slow (60-180s per container × 4 containers)
# - it's also fragile: if PyPI hiccups, your Airflow stack won't start
# - bakeed images are how Airflow gets deployed in production
#
# Updating deps: edit requirements-airflow.txt, then `docker compose build`.

FROM apache/airflow:2.10.4-python3.12

# Install system deps (none needed today, but this is where you'd add them).
# USER root
# RUN apt-get update && apt-get install -y <pkg> && rm -rf /var/lib/apt/lists/*
# USER airflow

# Copy and install Python deps. We do this as the airflow user (non-root)
# to match where pip extras would have landed (~/.local/lib/...).
COPY --chown=airflow:0 requirements-airflow.txt /opt/airflow/requirements-airflow.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements-airflow.txt