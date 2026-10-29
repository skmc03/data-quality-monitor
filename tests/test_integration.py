"""
tests/test_integration.py — End-to-end integration tests via subprocess.

Runs run_pipeline.py as a real subprocess so the full CLI entry point,
module imports, DuckDB writes, and file outputs are all exercised together.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

import pandas as pd
import pytest

# Path to the project root (one level above this tests/ directory)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PIPELINE_SCRIPT = os.path.join(_ROOT, "run_pipeline.py")
_BASELINE_CSV = os.path.join(_ROOT, "data", "sample_baseline.csv")
_BATCH_CSV = os.path.join(_ROOT, "data", "sample_batch.csv")


def _run(args: list[str], cwd: str = _ROOT) -> subprocess.CompletedProcess:
    """Run run_pipeline.py with given args; return CompletedProcess."""
    return subprocess.run(
        [sys.executable, _PIPELINE_SCRIPT] + args,
        capture_output=True,
        text=True,
        cwd=cwd,
    )


class TestEndToEndPipeline:
    """Full subprocess run of run_pipeline.py — exercises the complete path."""

    def test_pipeline_exits_zero_on_success(self, tmp_path):
        """Pipeline returns exit code 0 on a clean run."""
        db = str(tmp_path / "test.duckdb")
        alerts = str(tmp_path / "alerts.json")
        quarantine = str(tmp_path / "quarantine.csv")
        recon = str(tmp_path / "recon.json")

        result = _run([
            "--baseline", _BASELINE_CSV,
            "--batch", _BATCH_CSV,
            "--run-id", "INT_RUN_001",
            "--db", db,
            "--log-level", "WARNING",
        ])
        assert result.returncode == 0, f"Pipeline failed:\n{result.stderr}"

    def test_pipeline_produces_alerts_json(self, tmp_path):
        """alerts.json is created and contains at least one alert."""
        db = str(tmp_path / "test.duckdb")

        result = _run([
            "--baseline", _BASELINE_CSV,
            "--batch", _BATCH_CSV,
            "--run-id", "INT_RUN_002",
            "--db", db,
            "--log-level", "WARNING",
        ])
        assert result.returncode == 0, result.stderr

        alerts_path = os.path.join(_ROOT, "outputs", "alerts.json")
        assert os.path.exists(alerts_path), "alerts.json was not created"
        with open(alerts_path) as fh:
            alerts = json.load(fh)
        assert len(alerts) > 0, "Expected at least one alert"

    def test_pipeline_summary_in_stdout(self, tmp_path):
        """Run summary block appears in stdout."""
        db = str(tmp_path / "test.duckdb")

        result = _run([
            "--baseline", _BASELINE_CSV,
            "--batch", _BATCH_CSV,
            "--run-id", "INT_RUN_003",
            "--db", db,
            "--log-level", "WARNING",
        ])
        assert result.returncode == 0, result.stderr
        assert "DATA QUALITY MONITOR" in result.stdout
        assert "Run ID" in result.stdout
        assert "Valid rows" in result.stdout

    def test_idempotency_second_run_exits_two(self, tmp_path):
        """Running the same run_id twice (no --force) returns exit code 2."""
        db = str(tmp_path / "test.duckdb")
        common_args = [
            "--baseline", _BASELINE_CSV,
            "--batch", _BATCH_CSV,
            "--run-id", "INT_IDEM_001",
            "--db", db,
            "--log-level", "WARNING",
        ]

        first = _run(common_args)
        assert first.returncode == 0, first.stderr

        second = _run(common_args)
        assert second.returncode == 2, (
            f"Expected exit code 2 (skip) on second run, got {second.returncode}\n{second.stdout}"
        )

    def test_force_flag_reruns_completed(self, tmp_path):
        """--force on a completed run_id re-executes and exits 0."""
        db = str(tmp_path / "test.duckdb")
        common_args = [
            "--baseline", _BASELINE_CSV,
            "--batch", _BATCH_CSV,
            "--run-id", "INT_FORCE_001",
            "--db", db,
            "--log-level", "WARNING",
        ]

        first = _run(common_args)
        assert first.returncode == 0, first.stderr

        second = _run(common_args + ["--force"])
        assert second.returncode == 0, (
            f"Expected exit code 0 with --force, got {second.returncode}\n{second.stderr}"
        )

    def test_missing_baseline_file_exits_nonzero(self, tmp_path):
        """Pipeline exits non-zero if baseline CSV does not exist."""
        db = str(tmp_path / "test.duckdb")
        result = _run([
            "--baseline", str(tmp_path / "nonexistent.csv"),
            "--batch", _BATCH_CSV,
            "--run-id", "INT_MISSING_001",
            "--db", db,
            "--log-level", "WARNING",
        ])
        assert result.returncode != 0, "Expected non-zero exit for missing baseline"

    def test_reconciliation_report_produced(self, tmp_path):
        """reconciliation_report.json is produced and shows all checks passed."""
        db = str(tmp_path / "test.duckdb")

        result = _run([
            "--baseline", _BASELINE_CSV,
            "--batch", _BATCH_CSV,
            "--run-id", "INT_RECON_001",
            "--db", db,
            "--log-level", "WARNING",
        ])
        assert result.returncode == 0, result.stderr

        recon_path = os.path.join(_ROOT, "outputs", "reconciliation_report.json")
        assert os.path.exists(recon_path), "reconciliation_report.json not found"
        with open(recon_path) as fh:
            report = json.load(fh)
        assert report["summary"]["failed"] == 0, (
            f"Expected 0 failed checks, got {report['summary']['failed']}"
        )

    def test_quarantine_csv_has_rejected_rows(self, tmp_path):
        """quarantine.csv is written and contains rows with rejection_reason."""
        db = str(tmp_path / "test.duckdb")

        result = _run([
            "--baseline", _BASELINE_CSV,
            "--batch", _BATCH_CSV,
            "--run-id", "INT_QUAR_001",
            "--db", db,
            "--log-level", "WARNING",
        ])
        assert result.returncode == 0, result.stderr

        quarantine_path = os.path.join(_ROOT, "outputs", "quarantine.csv")
        assert os.path.exists(quarantine_path), "quarantine.csv not found"
        df = pd.read_csv(quarantine_path)
        assert len(df) > 0, "Expected quarantine rows"
        assert "rejection_reason" in df.columns
        assert df["rejection_reason"].notna().all(), "Some quarantine rows are missing rejection_reason"
