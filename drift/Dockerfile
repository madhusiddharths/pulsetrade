# drift/Dockerfile
# pulsetrade-drift — self-contained drift detection job.
#
# This is the SAME deployable unit you'll later run as a K8s pod. Locally it's
# spawned by Airflow's DockerOperator; in K8s it'd be a KubernetesPodOperator
# or CronJob. The image and entrypoint don't change — only the orchestrator.
#
# Build:  docker build -t pulsetrade-drift:latest ./drift
# Run standalone (test):
#   docker run --rm \
#     --env-file .env \
#     -e PULSETRADE_PG_HOST=host.docker.internal \
#     -e PULSETRADE_PG_PORT=5433 \
#     -e PULSETRADE_PG_USER=pulsetrade \
#     -e PULSETRADE_PG_PASSWORD=pulsetrade_dev \
#     -e PULSETRADE_PG_DB=pulsetrade \
#     -v "$(pwd)/drift/reports:/reports" \
#     pulsetrade-drift:latest

FROM python:3.11-slim

# System deps: psycopg2-binary ships wheels, but build-essential covers edge
# cases on slim. Kept minimal.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer caching — deps change less than code).
COPY requirements-drift.txt .
RUN pip install --no-cache-dir -r requirements-drift.txt

# Copy the drift modules.
COPY gold_data.py drift_detector.py drift_alert.py run_drift.py ./

# Default output dir for HTML reports (mounted as a volume at runtime).
ENV DRIFT_OUTPUT_DIR=/reports
RUN mkdir -p /reports

# Entrypoint runs the full drift job.
CMD ["python", "run_drift.py"]