# drift/learn_evidently.py
"""
Block 1 learning script — understand the Evidently API in isolation.

Goal: generate a drift report on toy data, with drift ARTIFICIALLY INJECTED
so we're guaranteed to see a positive "drift detected" result (not just an
empty "all clear"). Open the HTML, read the programmatic output, understand
what the tool gives you BEFORE wiring it to real gold data in Block 2.

Run:  python learn_evidently.py
Then: open drift_report.html in your browser
"""
from sklearn import datasets

# New API (Evidently 0.7.x):
#   - Report comes from the top-level package
#   - DataDriftPreset comes from evidently.presets
# (The OLD pre-0.7 API used `from evidently.report import Report` and
#  `from evidently.metric_preset import DataDriftPreset` — different, and
#  what most stale tutorials show. We are NOT using that.)
from evidently import Report
from evidently.presets import DataDriftPreset


def main():
    # ── 1. Load known-good sample data ───────────────────────────────────────
    # Iris: 150 rows, 4 numeric feature columns. Small, clean, well-understood.
    iris = datasets.load_iris(as_frame=True)
    df = iris.frame.copy()
    feature_cols = iris.feature_names  # the 4 numeric columns

    print(f"[data] loaded iris: {len(df)} rows, columns: {feature_cols}")

    # ── 2. Split into reference and current — SHUFFLED ───────────────────────
    # CRITICAL FIX: iris is ordered by species (rows 0-49, 50-99, 100-149 are
    # the three classes). A naive .iloc[:75] / .iloc[75:] split puts DIFFERENT
    # species in each half — so everything "drifts" because the two halves are
    # genuinely different populations. That's a windowing artifact, not real
    # feature drift.
    #
    # Shuffling first makes both halves the SAME population — now the only
    # drift will be what we deliberately inject. This mirrors the real rule:
    # reference and current windows must be sampled comparably, or the drift
    # signal is meaningless.
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    # Drop the target label — we only want to drift-check FEATURES here.
    # (In gold data, you'd similarly exclude ids, timestamps, etc.)
    df = df[feature_cols]

    reference = df.iloc[:75].copy()
    current = df.iloc[75:].copy()

    # ── 3. INJECT DRIFT into exactly TWO columns ─────────────────────────────
    current["sepal length (cm)"] = current["sepal length (cm)"] + 2.0
    current["petal width (cm)"] = current["petal width (cm)"] * 1.5

    print(f"[data] reference: {len(reference)} rows, current: {len(current)} rows")
    print("[data] injected drift into: sepal length (+2.0), petal width (×1.5)")
    
    # ── 4. Build and run the drift report ────────────────────────────────────
    # DataDriftPreset bundles per-column drift tests + a dataset-level summary.
    # include_tests=True adds explicit pass/fail conditions per column (useful
    # later for programmatic alerting).
    #
    # NOTE the argument order in the new API: run(current_data, reference_data).
    # Current FIRST, reference SECOND. This trips up everyone coming from the
    # old API where it was reference-first via keyword args.
    report = Report([DataDriftPreset()], include_tests=True)
    my_eval = report.run(current, reference)

    # ── 5. Save the HTML report ──────────────────────────────────────────────
    # This is the screenshot-magnet artifact: distribution overlays per column,
    # drift scores, the dataset-level verdict.
    my_eval.save_html("drift_report.html")
    print("[report] saved drift_report.html — open it in your browser")

    # ── 6. Get the programmatic result ───────────────────────────────────────
    # The HTML is for humans. For alerting (Block 2), we need the result as
    # data. .dict() gives a nested structure we can inspect.
    result_dict = my_eval.dict()

    # Print the top-level structure so you can SEE the shape. In Block 2 we'll
    # navigate this to extract "did column X drift?" for the Postgres alert.
    print("\n[result] top-level keys in the result dict:")
    import json
    print(json.dumps(result_dict, indent=2, default=str)[:2000])
    print("\n... (truncated — full structure explored in Block 2)")


if __name__ == "__main__":
    main()