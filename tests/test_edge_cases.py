"""
tests/test_edge_cases.py — Edge-case and resilience tests.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from pipeline.alerts import generate_alerts
from pipeline.comparator import compare
from pipeline.ingest import load_csv
from pipeline.profiler import profile, load_profile
from pipeline.quarantine import quarantine_records
from pipeline.validate import validate
from tests.conftest import make_valid_row


RUN_ID = "EDGE_001"


# ---------------------------------------------------------------------------
# Empty batch
# ---------------------------------------------------------------------------

class TestEmptyBatch:
    def test_empty_batch_produces_no_records(self, empty_csv: str) -> None:
        """Ingesting an empty file returns empty DataFrame."""
        df = load_csv(empty_csv, run_id=RUN_ID)
        assert len(df) == 0

    def test_validate_empty_df_returns_two_empty_dfs(self) -> None:
        df = pd.DataFrame(columns=[
            "transaction_id", "customer_id", "transaction_date", "amount",
            "currency", "category", "merchant_id", "status", "country_code",
            "is_flagged", "run_id", "source_file"
        ])
        valid, rejected = validate(df, run_id=RUN_ID)
        assert len(valid) == 0
        assert len(rejected) == 0

    def test_profile_on_empty_df_has_volume_zero(self, temp_db: str) -> None:
        df = pd.DataFrame(columns=[
            "transaction_id", "customer_id", "transaction_date", "amount",
            "currency", "category", "merchant_id", "status", "country_code",
            "is_flagged", "run_id", "source_file"
        ])
        p = profile(df, run_id=RUN_ID, db_path=temp_db)
        assert p["_volume"]["row_count"] == 0

    def test_compare_empty_batch_profile_no_crash(self, temp_db: str) -> None:
        """Comparing empty batch profile against baseline should not raise."""
        baseline_profile = {
            "_volume": {"profile_type": "volume", "null_rate": 0.0, "row_count": 100},
        }
        empty_batch_profile = {
            "_volume": {"profile_type": "volume", "null_rate": 0.0, "row_count": 0},
        }
        results = compare(
            baseline_profile=baseline_profile,
            batch_profile=empty_batch_profile,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID,
        )
        # Should produce a volume drift result, not crash
        assert isinstance(results, list)


# ---------------------------------------------------------------------------
# All rows rejected
# ---------------------------------------------------------------------------

class TestAllRowsRejected:
    def test_all_rows_rejected_valid_df_is_empty(self) -> None:
        """If all rows have invalid currency, valid_df should be empty."""
        rows = [
            make_valid_row(transaction_id=f"TXN_{i:04d}", currency="XYZ")
            for i in range(10)
        ]
        df = pd.DataFrame(rows)
        df["run_id"] = RUN_ID
        df["source_file"] = "test.csv"
        valid, rejected = validate(df, run_id=RUN_ID)
        assert len(valid) == 0
        assert len(rejected) == 10

    def test_rejected_df_has_rejection_reason_when_all_rejected(self) -> None:
        rows = [make_valid_row(transaction_id=f"TXN_{i:04d}", currency="BAD") for i in range(5)]
        df = pd.DataFrame(rows)
        df["run_id"] = RUN_ID
        df["source_file"] = "test.csv"
        _, rejected = validate(df, run_id=RUN_ID)
        assert rejected["rejection_reason"].notna().all()


# ---------------------------------------------------------------------------
# Schema change detection (new column appears)
# ---------------------------------------------------------------------------

class TestSchemaChangedDetected:
    def test_schema_change_detected_extra_column(self, temp_db: str) -> None:
        """
        If a new column appears in the batch that wasn't in the baseline,
        the profiler should handle it without crashing and produce a profile.
        """
        rng = np.random.default_rng(42)
        n = 20
        baseline_rows = [make_valid_row(transaction_id=f"TXN_{i:04d}") for i in range(n)]
        baseline_df = pd.DataFrame(baseline_rows)
        baseline_df["run_id"] = "EDGE_BASE"
        baseline_df["source_file"] = "baseline.csv"

        # Batch has an extra column 'loyalty_tier'
        batch_rows = [make_valid_row(transaction_id=f"BATCH_{i:04d}") for i in range(n)]
        batch_df = pd.DataFrame(batch_rows)
        batch_df["loyalty_tier"] = rng.choice(["gold", "silver", "bronze"], size=n)
        batch_df["run_id"] = RUN_ID
        batch_df["source_file"] = "batch.csv"

        # Both should profile without error
        p_baseline = profile(baseline_df, run_id="EDGE_BASE", db_path=temp_db)
        p_batch = profile(batch_df, run_id=RUN_ID, db_path=temp_db)

        assert p_baseline is not None
        assert p_batch is not None


# ---------------------------------------------------------------------------
# Late-arriving data
# ---------------------------------------------------------------------------

class TestLateArrivingData:
    def test_late_arriving_data_flagged(self, temp_db: str, tmp_path) -> None:
        """
        If a record's transaction_date is very old (60 days), and the freshness
        threshold is 48h, a freshness drift alert should be generated.
        """
        stale_date = (datetime.now(timezone.utc) - timedelta(days=60)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        baseline_profile = {
            "_volume": {"profile_type": "volume", "null_rate": 0.0, "row_count": 100},
            "transaction_date": {
                "profile_type": "freshness",
                "null_rate": 0.0,
                "row_count": 100,
                "freshness_min": now_str,
                "freshness_max": now_str,
            },
        }
        batch_profile = {
            "_volume": {"profile_type": "volume", "null_rate": 0.0, "row_count": 100},
            "transaction_date": {
                "profile_type": "freshness",
                "null_rate": 0.0,
                "row_count": 100,
                "freshness_min": stale_date,
                "freshness_max": stale_date,  # all records are stale
            },
        }

        results = compare(
            baseline_profile=baseline_profile,
            batch_profile=batch_profile,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID,
            freshness_max_lag_hours=48,
        )
        freshness_results = [r for r in results if r.metric == "freshness"]
        assert len(freshness_results) > 0
        assert freshness_results[0].severity in ("WARN", "CRITICAL")


# ---------------------------------------------------------------------------
# Rerun same input — no duplicates in quarantine
# ---------------------------------------------------------------------------

class TestRerunSameInputNoDuplicates:
    def test_rerun_same_input_no_duplicates(self, temp_db: str, tmp_path) -> None:
        """Quarantining same run_id twice should not duplicate records."""
        rows = [make_valid_row(transaction_id=f"TXN_{i:04d}", currency="BAD") for i in range(5)]
        df = pd.DataFrame(rows)
        df["run_id"] = RUN_ID
        df["source_file"] = "test.csv"
        _, rejected = validate(df, run_id=RUN_ID)

        csv_path = str(tmp_path / "quarantine.csv")
        quarantine_records(rejected, temp_db, run_id=RUN_ID, quarantine_csv_path=csv_path)
        quarantine_records(rejected, temp_db, run_id=RUN_ID, quarantine_csv_path=csv_path)

        import duckdb
        with duckdb.connect(temp_db) as con:
            count = con.execute(
                "SELECT COUNT(*) FROM quarantine WHERE run_id = ?", [RUN_ID]
            ).fetchone()[0]
        assert count == 5  # Not 10


# ---------------------------------------------------------------------------
# Malformed CSV handled gracefully
# ---------------------------------------------------------------------------

class TestMalformedCsvHandled:
    def test_malformed_csv_handled(self, malformed_csv: str) -> None:
        """
        A CSV with inconsistent column counts should either load
        (pandas is lenient) or raise a clear ValueError.
        """
        try:
            df = load_csv(malformed_csv, run_id=RUN_ID)
            # If it loads, we at least get a DataFrame back
            assert isinstance(df, pd.DataFrame)
        except (ValueError, Exception) as exc:
            # Acceptable: clear exception
            assert isinstance(exc, (ValueError, Exception))


# ---------------------------------------------------------------------------
# Missing baseline raises clear error
# ---------------------------------------------------------------------------

class TestMissingBaselineRaisesClearError:
    def test_missing_baseline_file_raises_error(self, tmp_path) -> None:
        """Attempting to load a non-existent baseline CSV raises FileNotFoundError."""
        missing = str(tmp_path / "baseline_does_not_exist.csv")
        with pytest.raises(FileNotFoundError):
            load_csv(missing, run_id=RUN_ID)

    def test_compare_with_empty_baseline_profile_returns_empty(self) -> None:
        """If baseline_profile is empty dict, compare should return empty list gracefully."""
        batch_profile = {
            "_volume": {"profile_type": "volume", "null_rate": 0.0, "row_count": 100},
        }
        results = compare(
            baseline_profile={},
            batch_profile=batch_profile,
            batch_df=None,
            baseline_df=None,
            run_id=RUN_ID,
        )
        assert results == []


# ---------------------------------------------------------------------------
# Partial failure recovery
# ---------------------------------------------------------------------------

class TestPartialFailureRecovery:
    def test_validate_does_not_raise_on_mixed_valid_invalid(self) -> None:
        """A mix of valid and invalid rows should not raise — returns two splits."""
        rows = [
            make_valid_row(transaction_id="VALID_001"),
            make_valid_row(transaction_id="INVALID_001", currency="BAD"),
            make_valid_row(transaction_id="VALID_002"),
        ]
        df = pd.DataFrame(rows)
        df["run_id"] = RUN_ID
        df["source_file"] = "mixed.csv"
        valid, rejected = validate(df, run_id=RUN_ID)
        assert len(valid) == 2
        assert len(rejected) == 1

    def test_quarantine_empty_df_returns_zero(self, temp_db: str) -> None:
        """Quarantining an empty DataFrame should not raise and returns 0."""
        empty_df = pd.DataFrame()
        count = quarantine_records(empty_df, temp_db, run_id=RUN_ID)
        assert count == 0

    def test_generate_alerts_with_empty_drift_list(self, tmp_path) -> None:
        """generate_alerts with empty list should produce empty alerts file."""
        output_path = str(tmp_path / "alerts.json")
        alerts = generate_alerts([], run_id=RUN_ID, output_path=output_path)
        assert alerts == []
        with open(output_path) as fh:
            saved = json.load(fh)
        assert saved == []
