"""Unit tests for the Evidently 0.7.x result-dict navigation — the exact shape
documented (from a real .dict() dump) in drift/drift_detector.py."""
from drift_detector import extract_drift_results

RESULT_DICT = {
    "metrics": [
        {
            "config": {"type": "evidently:metric_v2:DriftedColumnsCount"},
            "value": {"count": 1, "share": 0.5},
        },
        {
            "config": {
                "type": "evidently:metric_v2:ValueDrift",
                "column": "price_stddev",
                "method": "ks",
                "threshold": 0.05,
            },
            "value": 0.01,  # p < threshold -> drifted
        },
        {
            "config": {
                "type": "evidently:metric_v2:ValueDrift",
                "column": "mean_change_pct",
                "method": "ks",
                "threshold": 0.05,
            },
            "value": 0.5,  # p >= threshold -> stable
        },
    ]
}


def test_extract_navigates_summary_and_columns():
    summary = extract_drift_results(RESULT_DICT)
    assert summary["n_columns"] == 2
    assert summary["n_drifted"] == 1
    assert summary["share_drifted"] == 0.5
    assert summary["drifted_columns"] == ["price_stddev"]


def test_drift_rule_is_p_value_below_threshold():
    summary = extract_drift_results(RESULT_DICT)
    assert summary["columns"]["price_stddev"]["drifted"] is True
    assert summary["columns"]["mean_change_pct"]["drifted"] is False


def test_extract_tolerates_empty_result():
    summary = extract_drift_results({})
    assert summary["n_columns"] == 0
    assert summary["drifted_columns"] == []
