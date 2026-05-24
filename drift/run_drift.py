# drift/run_drift.py
"""
Drift detection entrypoint — runs in the pulsetrade-drift container (spawned
by the Airflow DAG), or standalone for testing.

Flow:
  1. pull reference + current windows from gold (current has injected drift)
  2. run Evidently DataDriftPreset
  3. save a TIMESTAMPED HTML report to the output dir
  4. extract results, evaluate thresholds, write a drift_alerts row to Postgres

Env vars required (injected by DockerOperator, or from .env when standalone):
  DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
  PULSETRADE_PG_HOST, PULSETRADE_PG_PORT, PULSETRADE_PG_USER,
  PULSETRADE_PG_PASSWORD, PULSETRADE_PG_DB

Optional:
  DRIFT_OUTPUT_DIR   — where to write HTML (default: /reports in container)
  DRIFT_INJECT       — "true"/"false", whether to inject demo drift (default true)
"""
import os
from datetime import datetime, timezone
from pathlib import Path

# Load .env only when running standalone (not in container). In the container,
# env comes from the DockerOperator. load_dotenv is a no-op if the file is absent.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except Exception:
    pass

from gold_data import get_reference_window, get_current_window, DRIFT_TARGET_COLUMN
from drift_detector import run_drift_report, extract_drift_results, format_summary
from drift_alert import init_drift_table, write_drift_alert, evaluate_alert


def main():
    output_dir = os.environ.get("DRIFT_OUTPUT_DIR", "/reports")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    inject = os.environ.get("DRIFT_INJECT", "true").lower() == "true"

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    html_path = os.path.join(output_dir, f"gold_drift_report_{ts}.html")

    print(f"[run_drift] pulling windows (inject_drift={inject})...")
    reference = get_reference_window()
    current = get_current_window(inject_drift=inject)
    print(f"[run_drift] reference={reference.shape}, current={current.shape}")
    print(
        f"[run_drift] sentiment mean — ref={reference[DRIFT_TARGET_COLUMN].mean():.4f}, "
        f"cur={current[DRIFT_TARGET_COLUMN].mean():.4f}"
    )

    print("[run_drift] running Evidently...")
    result_dict = run_drift_report(reference, current, html_path=html_path)

    summary = extract_drift_results(result_dict)
    print("\n" + format_summary(summary))

    # Ensure table exists, then persist this run.
    init_drift_table()
    should_alert, reason = evaluate_alert(summary)
    row_id = write_drift_alert(summary, report_path=html_path)

    print(f"\n[run_drift] drift_alerts row #{row_id} written")
    print(f"[run_drift] alerted={should_alert} - {reason}")
    print(f"[run_drift] HTML report -> {html_path}")

    # Exit 0 even when alerted: drift is a signal to RECORD, not a pipeline
    # failure. If you'd rather the DAG task go red on drift, return 1 here.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())