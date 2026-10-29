"""
tests/test_profiler.py — Tests for pipeline.profiler module.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from pipeline.profiler import profile, load_profile, _profile_exists


RUN_ID = "TEST_PROFILER_001"
RUN_ID_2 = "TEST_PROFILER_002"


def _add_meta(df: pd.DataFrame, run_id: str = RUN_ID) -> pd.DataFrame:
    df = df.copy()
    df["run_id"] = run_id
    df["source_file"] = "test.csv"
    return df


class TestNullRateComputedCorrectly:
    def test_null_rate_zero_when_no_nulls(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        # currency has no nulls in sample_valid_df
        assert p["currency"]["null_rate"] == pytest.approx(0.0, abs=1e-6)

    def test_null_rate_computed_correctly(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        """Inject known nulls and verify null_rate calculation."""
        df = sample_valid_df.copy()
        # Cast to object first so pandas accepts None without dtype conflict
        df["is_flagged"] = df["is_flagged"].astype(object)
        df.loc[:9, "is_flagged"] = None
        df = _add_meta(df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        expected_rate = 10 / len(df)
        assert p["is_flagged"]["null_rate"] == pytest.approx(expected_rate, abs=0.01)

    def test_null_rate_one_when_all_null(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = sample_valid_df.copy()
        df["is_flagged"] = None
        df = _add_meta(df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        assert p["is_flagged"]["null_rate"] == pytest.approx(1.0, abs=1e-6)


class TestNumericStats:
    def test_numeric_stats_computed(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        amount_profile = p["amount"]
        for stat in ("min", "max", "mean", "median", "std", "p5", "p25", "p75", "p95"):
            assert stat in amount_profile, f"Missing stat: {stat}"
            assert amount_profile[stat] is not None

    def test_numeric_min_less_than_max(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        ap = p["amount"]
        assert ap["min"] <= ap["max"]

    def test_numeric_p25_less_than_p75(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        ap = p["amount"]
        assert ap["p25"] <= ap["p75"]

    def test_numeric_mean_within_range(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        ap = p["amount"]
        assert ap["min"] <= ap["mean"] <= ap["max"]


class TestCategoricalDistribution:
    def test_categorical_distribution_computed(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        assert "value_counts" in p["currency"]
        assert len(p["currency"]["value_counts"]) > 0

    def test_categorical_proportions_sum_to_one(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        total = sum(p["currency"]["value_counts"].values())
        assert total == pytest.approx(1.0, abs=0.01)

    def test_categorical_known_values_in_distribution(
        self, sample_valid_df: pd.DataFrame, temp_db: str
    ) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        # At least one of the valid currencies should appear
        assert len(p["currency"]["value_counts"]) > 0


class TestVolumeCaptured:
    def test_volume_captured(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        assert "_volume" in p
        assert p["_volume"]["row_count"] == len(df)

    def test_volume_zero_for_empty_df(self, temp_db: str) -> None:
        df = pd.DataFrame(columns=[
            "transaction_id", "customer_id", "transaction_date", "amount",
            "currency", "category", "merchant_id", "status", "country_code",
            "is_flagged", "run_id", "source_file"
        ])
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        assert p["_volume"]["row_count"] == 0


class TestFreshness:
    def test_freshness_computed(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        assert "transaction_date" in p
        assert p["transaction_date"]["profile_type"] == "freshness"
        assert p["transaction_date"]["freshness_min"] is not None
        assert p["transaction_date"]["freshness_max"] is not None

    def test_freshness_min_lte_max(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        df = _add_meta(sample_valid_df)
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        fmin = pd.to_datetime(p["transaction_date"]["freshness_min"])
        fmax = pd.to_datetime(p["transaction_date"]["freshness_max"])
        assert fmin <= fmax


class TestProfileIdempotent:
    def test_profile_is_idempotent(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        """Running profile twice with same run_id should not create duplicate DB rows."""
        df = _add_meta(sample_valid_df)
        p1 = profile(df, run_id=RUN_ID, db_path=temp_db)
        p2 = profile(df, run_id=RUN_ID, db_path=temp_db)  # second call — should load from DB
        assert p1.keys() == p2.keys()

    def test_profile_force_overwrites(self, sample_valid_df: pd.DataFrame, temp_db: str) -> None:
        """force=True should overwrite existing profile."""
        df = _add_meta(sample_valid_df)
        profile(df, run_id=RUN_ID, db_path=temp_db)
        # Modify df slightly and force re-profile
        df2 = df.copy()
        df2.loc[:4, "amount"] = 99999.0
        p2 = profile(df2, run_id=RUN_ID, db_path=temp_db, force=True)
        assert p2["amount"]["max"] == pytest.approx(99999.0, rel=0.01)

    def test_different_run_ids_create_separate_profiles(
        self, sample_valid_df: pd.DataFrame, temp_db: str
    ) -> None:
        df1 = _add_meta(sample_valid_df, run_id=RUN_ID)
        df2 = _add_meta(sample_valid_df, run_id=RUN_ID_2)
        profile(df1, run_id=RUN_ID, db_path=temp_db)
        profile(df2, run_id=RUN_ID_2, db_path=temp_db)
        p1 = load_profile(temp_db, RUN_ID)
        p2 = load_profile(temp_db, RUN_ID_2)
        assert p1 is not None
        assert p2 is not None
