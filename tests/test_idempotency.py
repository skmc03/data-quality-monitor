"""
tests/test_idempotency.py — Tests for pipeline.idempotency module.
"""

from __future__ import annotations

import duckdb
import pytest

from pipeline.idempotency import (
    check_run_exists,
    guard_idempotent,
    is_already_completed,
    purge_run,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
)
from pipeline.logger import RunTracker, _DDL_PIPELINE_RUNS


RUN_ID = "TEST_IDEM_001"


def _register_run(db_path: str, run_id: str, status: str = "COMPLETED") -> None:
    """Helper: directly insert a pipeline_runs row for testing."""
    from datetime import datetime, timezone
    with duckdb.connect(db_path) as con:
        con.execute(_DDL_PIPELINE_RUNS)
        con.execute(
            """
            INSERT INTO pipeline_runs (run_id, start_time, status)
            VALUES (?, ?, ?)
            ON CONFLICT (run_id) DO UPDATE SET status = excluded.status
            """,
            [run_id, datetime.now(timezone.utc), status],
        )


class TestCheckRunExists:
    def test_returns_none_for_unknown_run_id(self, temp_db: str) -> None:
        result = check_run_exists(temp_db, "NONEXISTENT_RUN")
        assert result is None

    def test_returns_status_for_known_run_id(self, temp_db: str) -> None:
        _register_run(temp_db, RUN_ID, "COMPLETED")
        result = check_run_exists(temp_db, RUN_ID)
        assert result == "COMPLETED"

    def test_returns_running_status(self, temp_db: str) -> None:
        _register_run(temp_db, RUN_ID, "RUNNING")
        assert check_run_exists(temp_db, RUN_ID) == "RUNNING"


class TestIsAlreadyCompleted:
    def test_returns_false_for_new_run(self, temp_db: str) -> None:
        assert is_already_completed(temp_db, "BRAND_NEW") is False

    def test_returns_true_for_completed_run(self, temp_db: str) -> None:
        _register_run(temp_db, RUN_ID, "COMPLETED")
        assert is_already_completed(temp_db, RUN_ID) is True

    def test_returns_false_for_failed_run(self, temp_db: str) -> None:
        _register_run(temp_db, RUN_ID, "FAILED")
        assert is_already_completed(temp_db, RUN_ID) is False


class TestGuardIdempotent:
    def test_new_run_proceeds(self, temp_db: str) -> None:
        """A brand-new run_id should return True (proceed)."""
        result = guard_idempotent(temp_db, "BRAND_NEW_RUN")
        assert result is True

    def test_completed_run_skipped(self, temp_db: str) -> None:
        """A COMPLETED run_id should return False (skip) without force."""
        _register_run(temp_db, RUN_ID, "COMPLETED")
        result = guard_idempotent(temp_db, RUN_ID, force=False)
        assert result is False

    def test_force_flag_reruns_completed(self, temp_db: str) -> None:
        """With force=True, a COMPLETED run should return True."""
        _register_run(temp_db, RUN_ID, "COMPLETED")
        result = guard_idempotent(temp_db, RUN_ID, force=True)
        assert result is True

    def test_failed_run_retried_automatically(self, temp_db: str) -> None:
        """A FAILED run is automatically retried (returns True)."""
        _register_run(temp_db, RUN_ID, "FAILED")
        result = guard_idempotent(temp_db, RUN_ID, force=False)
        assert result is True

    def test_running_run_skipped(self, temp_db: str) -> None:
        """A RUNNING run (possibly stale) is skipped without force."""
        _register_run(temp_db, RUN_ID, "RUNNING")
        result = guard_idempotent(temp_db, RUN_ID, force=False)
        assert result is False


class TestPurgeRun:
    def test_purge_removes_pipeline_run(self, temp_db: str) -> None:
        _register_run(temp_db, RUN_ID, "COMPLETED")
        purge_run(temp_db, RUN_ID)
        assert check_run_exists(temp_db, RUN_ID) is None

    def test_purge_idempotent_on_nonexistent_run(self, temp_db: str) -> None:
        """Purging a non-existent run should not raise."""
        purge_run(temp_db, "NONEXISTENT_RUN_XYZ")  # should not raise


class TestRerunSkipsProcessing:
    def test_rerun_skips_processing(self, temp_db: str) -> None:
        """guard_idempotent returns False for already-completed run without force."""
        _register_run(temp_db, RUN_ID, "COMPLETED")
        should_proceed = guard_idempotent(temp_db, RUN_ID, force=False)
        assert should_proceed is False

    def test_no_duplicate_rows_on_rerun(self, temp_db: str) -> None:
        """RunTracker.start() with ON CONFLICT should not create duplicate rows."""
        tracker = RunTracker(temp_db, RUN_ID)
        tracker.start(source_file="test.csv", input_count=100)
        tracker.start(source_file="test.csv", input_count=100)  # second call

        with duckdb.connect(temp_db) as con:
            count = con.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE run_id = ?", [RUN_ID]
            ).fetchone()[0]
        assert count == 1

    def test_force_flag_reruns(self, temp_db: str) -> None:
        """With force=True, guard_idempotent returns True even for completed run."""
        _register_run(temp_db, RUN_ID, "COMPLETED")
        result = guard_idempotent(temp_db, RUN_ID, force=True)
        assert result is True
