# drift/drift_detector.py
"""
Evidently drift detection — pinned to the 0.7.x API and result structure.

Result-extraction logic is written against the EXACT dict shape confirmed
from a real .dict() dump on Evidently 0.7.21. The metrics list contains two
entry types, distinguished by config.type:

  evidently:metric_v2:DriftedColumnsCount  → dataset summary
      value = {"count": <float>, "share": <float>}

  evidently:metric_v2:ValueDrift           → one per column
      config = {"column": <name>, "method": <str>, "threshold": <float>}
      value  = <raw p-value as float>      ← NOTE: no boolean!

CRITICAL: the output has NO pre-computed "drifted: true/false". For each
column we compute drifted = (p_value < threshold) ourselves. For K-S p-value,
LOWER means MORE drift (low p-value = distributions unlikely to be the same).
"""
from typing import Any

import pandas as pd
from evidently import Report
from evidently.presets import DataDriftPreset


def run_drift_report(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    html_path: str = "gold_drift_report.html",
) -> dict:
    """
    Run DataDriftPreset comparing current vs reference, save HTML, return the
    result as a dict (the structure extract_drift_results() navigates).

    Argument order to .run() is (current, reference) in the 0.7.x API —
    current FIRST. This trips up everyone coming from the old API.
    """
    report = Report([DataDriftPreset()], include_tests=True)
    my_eval = report.run(current_df, reference_df)

    my_eval.save_html(html_path)
    print(f"[drift] saved HTML report → {html_path}")

    return my_eval.dict()


def extract_drift_results(result_dict: dict) -> dict:
    """
    Navigate the Evidently 0.7.x result dict into a clean, actionable summary.

    Returns:
        {
          "n_columns": int,
          "n_drifted": int,
          "share_drifted": float,
          "columns": {
             "<col>": {"p_value": float, "threshold": float,
                       "method": str, "drifted": bool},
             ...
          },
          "drifted_columns": ["<col>", ...],
        }
    """
    metrics = result_dict.get("metrics", [])

    summary = {"n_columns": 0, "n_drifted": 0, "share_drifted": 0.0,
               "columns": {}, "drifted_columns": []}

    for m in metrics:
        mtype = m.get("config", {}).get("type", "")

        # Dataset-level summary entry
        if mtype.endswith("DriftedColumnsCount"):
            val = m.get("value", {}) or {}
            summary["n_drifted"] = int(val.get("count", 0))
            summary["share_drifted"] = float(val.get("share", 0.0))

        # Per-column entry
        elif mtype.endswith("ValueDrift"):
            cfg = m.get("config", {})
            col = cfg.get("column")
            threshold = float(cfg.get("threshold", 0.05))
            method = cfg.get("method", "unknown")
            p_value = float(m.get("value", 1.0))

            # The rule: for p-value methods, drift = p_value < threshold.
            drifted = p_value < threshold

            summary["columns"][col] = {
                "p_value": p_value,
                "threshold": threshold,
                "method": method,
                "drifted": drifted,
            }
            if drifted:
                summary["drifted_columns"].append(col)

    summary["n_columns"] = len(summary["columns"])
    return summary


def format_summary(summary: dict) -> str:
    """Human-readable one-screen summary for logs / terminal."""
    lines = [
        "─" * 60,
        f"DRIFT SUMMARY: {summary['n_drifted']}/{summary['n_columns']} columns "
        f"drifted (share={summary['share_drifted']:.2f})",
        "─" * 60,
    ]
    for col, d in summary["columns"].items():
        flag = "🔴 DRIFT" if d["drifted"] else "🟢 ok   "
        lines.append(
            f"  {flag}  {col:<24} p={d['p_value']:.4g} "
            f"(thr={d['threshold']}, {d['method']})"
        )
    return "\n".join(lines)