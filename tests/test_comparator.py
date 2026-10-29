"""
tests/test_comparator.py — Tests for pipeline.comparator module.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

from pipeline.comparator import compare, DriftResult, _compute_psi
from pipeline.profiler import profile


RUN_ID_BASELINE = "COMP_BASELINE"
RUN_ID_BATCH = "COMP_BATCH"


def _make_profile(
    volume: int = 100,
    amount_mean: float = 500.0,
    amount_std: float = 100.0,
    null_rate_is_flagged: float = 0.02,
    currencies: dict = None,
    freshness_max_iso: str = None,
) -> Dict[str, Any]:
    """Build a minimal profile dict for comparator tests."""
    if freshness_max_iso is None:
        freshness_max_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if currencies is None:
        currencies = {"USD": 0.55, "EUR": 0.20, "GBP": 0.15, "INR": 0.10}

    return {
        "_volume": {"profile_type": "volume", "null_rate": 0.0, "row_count": volume},
        "amount": {
            "profile_type": "numeric",
            "null_rate": 0.0,
            "row_count": volume,
            "min": amount_mean - 2 * amount_std,
            "max": amount_mean + 2 * amount_std,
            "mean": amount_mean,
            "median": amount_mean,
            "std": amount_std,
            "p5": amount_mean - 1.5 * amount_std,
            "p25": amount_mean - 0.5 * amount_std,
            "p75": amount_mean + 0.5 * amount_std,
            "p95": amount_mean + 1.5 * amount_std,
        },
        "currency": {
            "profile_type": "categorical",
            "null_rate": 0.0,
            "row_count": volume,
            "value_counts": currencies,
        },
        "is_flagged": {
            "profile_type": "categorical",
            "null_rate": null_rate_is_flagged,
            "row_count": volume,
            "value_counts": {"False": 0.95 - null_rate_is_flagged, "True": 0.03},
        },
        "transaction_date": {
            "profile_type": "freshness",
            "null_rate": 0.0,
            "row_count": volume,
            "freshness_min": freshness_max_iso,
            "freshness_max": freshness_max_iso,
        },
    }


class TestNoDriftWhenIdentical:
    def test_no_drift_when_identical(self) -> None:
        """Identical baseline and batch profiles should produce no WARN/CRITICAL results."""
        baseline_p = _make_profile()
        batch_p = _make_profile()
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
        )
        high_severity = [r for r in results if r.severity in ("WARN", "CRITICAL")]
        assert len(high_severity) == 0, f"Unexpected alerts: {high_severity}"


class TestNullRateDrift:
    def test_null_rate_drift_detected(self) -> None:
        """Null rate increase from 2% to 15% should trigger a WARN or CRITICAL."""
        baseline_p = _make_profile(null_rate_is_flagged=0.02)
        batch_p = _make_profile(null_rate_is_flagged=0.15)
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
        )
        null_results = [r for r in results if r.metric == "null_rate" and r.column == "is_flagged"]
        assert len(null_results) > 0, "Expected null_rate drift for is_flagged"
        assert null_results[0].severity in ("WARN", "CRITICAL")

    def test_null_rate_delta_below_threshold_not_flagged(self) -> None:
        """Small null rate change (< 5%) should not trigger an alert."""
        baseline_p = _make_profile(null_rate_is_flagged=0.02)
        batch_p = _make_profile(null_rate_is_flagged=0.04)  # delta = 2%, below 5% threshold
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
            null_rate_delta_threshold=0.05,
        )
        null_results = [
            r for r in results
            if r.metric == "null_rate" and r.column == "is_flagged" and r.severity != "INFO"
        ]
        assert len(null_results) == 0


class TestVolumeDrop:
    def test_volume_drop_detected(self) -> None:
        """Volume drop of 50% should trigger WARN or CRITICAL."""
        baseline_p = _make_profile(volume=1000)
        batch_p = _make_profile(volume=500)
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
            volume_delta_pct_threshold=0.20,
        )
        vol_results = [r for r in results if r.metric == "volume"]
        assert len(vol_results) > 0
        assert vol_results[0].severity in ("WARN", "CRITICAL")

    def test_small_volume_change_not_flagged(self) -> None:
        """Volume change of 5% (below 20% threshold) should not trigger alert."""
        baseline_p = _make_profile(volume=1000)
        batch_p = _make_profile(volume=1050)
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
            volume_delta_pct_threshold=0.20,
        )
        vol_results = [r for r in results if r.metric == "volume" and r.severity != "INFO"]
        assert len(vol_results) == 0


class TestDistributionPSI:
    def test_distribution_psi_drift_detected(self, temp_db: str) -> None:
        """Large shift in amount distribution should produce PSI drift alert."""
        rng = np.random.default_rng(42)
        baseline_df = pd.DataFrame({"amount": rng.normal(500, 100, size=200)})
        batch_df = pd.DataFrame({"amount": rng.normal(8000, 2000, size=200)})  # big shift

        baseline_p = profile(
            _add_meta_df(baseline_df), run_id=RUN_ID_BASELINE, db_path=temp_db
        )
        batch_p = profile(
            _add_meta_df(batch_df, run_id=RUN_ID_BATCH), run_id=RUN_ID_BATCH, db_path=temp_db
        )

        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=batch_df,
            baseline_df=baseline_df,
            run_id=RUN_ID_BATCH,
            psi_threshold=0.2,
        )
        psi_results = [r for r in results if r.metric == "distribution_psi"]
        assert len(psi_results) > 0
        assert any(r.severity in ("WARN", "CRITICAL") for r in psi_results)

    def test_psi_zero_for_identical_distributions(self) -> None:
        """PSI should be very low for near-identical distributions."""
        rng = np.random.default_rng(42)
        vals = rng.normal(500, 100, size=1000)
        psi = _compute_psi(vals, vals)
        assert psi < 0.01


class TestNewCategory:
    def test_new_category_detected(self) -> None:
        """A new currency value in batch not in baseline should trigger new_category."""
        baseline_p = _make_profile(currencies={"USD": 0.80, "EUR": 0.20})
        # Batch has a new currency "CNY"
        batch_p = _make_profile(currencies={"USD": 0.70, "EUR": 0.20, "CNY": 0.10})
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
        )
        new_cat = [r for r in results if r.metric == "new_category" and r.column == "currency"]
        assert len(new_cat) > 0
        assert new_cat[0].current_value == "CNY"


class TestMissingCategory:
    def test_missing_category_detected(self) -> None:
        """A currency present in baseline but absent in batch should trigger missing_category."""
        baseline_p = _make_profile(currencies={"USD": 0.70, "EUR": 0.20, "GBP": 0.10})
        batch_p = _make_profile(currencies={"USD": 0.80, "EUR": 0.20})  # GBP missing
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
        )
        missing = [r for r in results if r.metric == "missing_category" and r.column == "currency"]
        assert len(missing) > 0
        assert missing[0].baseline_value == "GBP"


class TestFreshnessLag:
    def test_freshness_lag_detected(self) -> None:
        """If most recent record is 72 hours old (> 48h threshold), alert triggered."""
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=72)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        baseline_p = _make_profile()
        batch_p = _make_profile(freshness_max_iso=stale_time)
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
            freshness_max_lag_hours=48,
        )
        fresh_results = [r for r in results if r.metric == "freshness"]
        assert len(fresh_results) > 0
        assert fresh_results[0].severity in ("WARN", "CRITICAL")

    def test_fresh_data_no_freshness_alert(self) -> None:
        """Recent data (2h old) should not trigger freshness alert."""
        recent_time = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        baseline_p = _make_profile()
        batch_p = _make_profile(freshness_max_iso=recent_time)
        results = compare(
            baseline_profile=baseline_p,
            batch_profile=batch_p,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID_BATCH,
            freshness_max_lag_hours=48,
        )
        fresh_results = [r for r in results if r.metric == "freshness"]
        assert len(fresh_results) == 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_meta_df(df: pd.DataFrame, run_id: str = RUN_ID_BASELINE) -> pd.DataFrame:
    df = df.copy()
    df["run_id"] = run_id
    df["source_file"] = "test.csv"
    # Add minimal columns for profiler
    from datetime import timezone
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    n = len(df)
    for col in ["currency", "category", "merchant_id", "status", "country_code", "is_flagged"]:
        if col not in df.columns:
            df[col] = "USD" if col == "currency" else None
    if "transaction_date" not in df.columns:
        df["transaction_date"] = now_str
    return df
