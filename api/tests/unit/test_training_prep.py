"""Unit tests for the pure data-prep logic in agent/model_training.py."""
import pandas as pd

from agent.model_training import (
    FEATURE_COLS,
    _label_anomalies_zscore,
    _split_train_eval,
    _to_feature_matrix,
)


def _row(ticker="AAPL", window_start="2026-07-01T00:00:00Z", change=0.1):
    return {
        "ticker": ticker,
        "window_start": window_start,
        "mean_change_pct": change,
        "mean_intraday_range": 1.0,
        "price_stddev": 0.5,
        "mean_news_sentiment": 0.0,
        "news_article_count": 3,
    }


def test_to_feature_matrix_drops_rows_with_null_features():
    rows = [_row(), {**_row(), "price_stddev": None}]
    df = _to_feature_matrix(rows)
    assert len(df) == 1
    assert list(df[FEATURE_COLS].dtypes.astype(str)) != ["object"] * len(FEATURE_COLS)


def test_to_feature_matrix_handles_missing_column_and_empty():
    rows = [{k: v for k, v in _row().items() if k != "news_article_count"}]
    assert _to_feature_matrix(rows).empty  # NaN feature column -> row dropped
    assert _to_feature_matrix([]).empty


def test_split_train_eval_is_chronological():
    rows = [
        _row(window_start=f"2026-07-0{d}T00:00:00Z", change=float(d))
        for d in range(1, 6)
    ]
    df = pd.DataFrame(rows)
    train, eval_ = _split_train_eval(df, eval_hours=24)
    # cutoff = max - 24h; the boundary row lands in eval (>= cutoff)
    assert len(train) == 3 and len(eval_) == 2
    assert train["window_start"].max() < eval_["window_start"].min()


def test_zscore_labels_flag_only_the_outlier():
    values = [0.1, 0.11, 0.09, 0.1, 0.12, 0.1, 0.11, 0.09, 0.1, 5.0]
    df = pd.DataFrame(
        [_row(change=v, window_start=f"2026-07-01T00:{i:02d}:00Z") for i, v in enumerate(values)]
    )
    labels = _label_anomalies_zscore(df, z_threshold=2.5)
    assert labels.sum() == 1
    assert bool(labels.iloc[-1]) is True


def test_zscore_constant_series_never_flags():
    df = pd.DataFrame([_row(change=0.1) for _ in range(10)])
    labels = _label_anomalies_zscore(df)
    assert labels.sum() == 0
