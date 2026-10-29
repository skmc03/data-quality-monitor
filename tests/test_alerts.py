"""
tests/test_alerts.py — Tests for pipeline.alerts module.
"""

from __future__ import annotations

import hashlib
import json
import os

import pytest

from pipeline.alerts import generate_alerts, load_alerts, _deterministic_alert_id
from pipeline.comparator import DriftResult


RUN_ID = "TEST_ALERTS_001"


def _make_drift(
    column: str = "is_flagged",
    metric: str = "null_rate",
    baseline_value: object = 0.02,
    current_value: object = 0.15,
    delta: float = 0.13,
    severity: str = "CRITICAL",
    threshold: float = 0.05,
    description: str = "Test drift result",
) -> DriftResult:
    return DriftResult(
        column=column,
        metric=metric,
        baseline_value=baseline_value,
        current_value=current_value,
        delta=delta,
        severity=severity,
        threshold=threshold,
        description=description,
    )


class TestCriticalAlertGenerated:
    def test_critical_alert_generated_for_high_drift(self, tmp_path) -> None:
        """A CRITICAL DriftResult produces a CRITICAL alert."""
        dr = _make_drift(severity="CRITICAL")
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        critical = [a for a in alerts if a["severity"] == "CRITICAL"]
        assert len(critical) > 0

    def test_warn_alert_generated_for_moderate_drift(self, tmp_path) -> None:
        dr = _make_drift(severity="WARN", delta=0.07, current_value=0.09)
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        warn = [a for a in alerts if a["severity"] == "WARN"]
        assert len(warn) > 0

    def test_info_alert_included(self, tmp_path) -> None:
        dr = _make_drift(severity="INFO", delta=0.01, current_value=0.03)
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        info = [a for a in alerts if a["severity"] == "INFO"]
        assert len(info) > 0


class TestAlertHasExplanationText:
    def test_alert_has_explanation_text(self, tmp_path) -> None:
        """Each alert must have a non-empty 'explanation' string."""
        dr = _make_drift(severity="CRITICAL")
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        for alert in alerts:
            assert "explanation" in alert
            assert isinstance(alert["explanation"], str)
            assert len(alert["explanation"]) > 20, "Explanation too short"

    def test_explanation_mentions_column(self, tmp_path) -> None:
        """Explanation text should reference the column name."""
        dr = _make_drift(column="is_flagged", metric="null_rate", severity="CRITICAL")
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        assert "is_flagged" in alerts[0]["explanation"]

    def test_explanation_for_volume_metric(self, tmp_path) -> None:
        dr = _make_drift(column="_volume", metric="volume", severity="WARN", delta=-0.30)
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        assert alerts[0]["explanation"]  # not empty

    def test_explanation_for_psi_metric(self, tmp_path) -> None:
        dr = _make_drift(
            column="amount", metric="distribution_psi",
            baseline_value=500.0, current_value=8000.0, delta=0.35, severity="CRITICAL"
        )
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        assert "PSI" in alerts[0]["explanation"]

    def test_explanation_for_new_category(self, tmp_path) -> None:
        dr = _make_drift(
            column="currency", metric="new_category",
            baseline_value=None, current_value="JPY", delta=0.0, severity="WARN"
        )
        alerts = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts.json")
        )
        assert "JPY" in alerts[0]["explanation"]


class TestAlertIdDeterminism:
    def test_alert_id_is_deterministic(self, tmp_path) -> None:
        """Same run_id + column + metric always produces the same alert_id."""
        dr = _make_drift(column="is_flagged", metric="null_rate")
        alerts1 = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts1.json")
        )
        alerts2 = generate_alerts(
            [dr], run_id=RUN_ID, output_path=str(tmp_path / "alerts2.json")
        )
        assert alerts1[0]["alert_id"] == alerts2[0]["alert_id"]

    def test_alert_id_matches_sha256(self) -> None:
        """Alert ID equals SHA-256(run_id::column::metric)."""
        run_id = "MY_RUN"
        column = "amount"
        metric = "distribution_psi"
        expected = hashlib.sha256(f"{run_id}::{column}::{metric}".encode()).hexdigest()
        computed = _deterministic_alert_id(run_id, column, metric)
        assert computed == expected

    def test_different_columns_produce_different_ids(self) -> None:
        id1 = _deterministic_alert_id(RUN_ID, "col_a", "null_rate")
        id2 = _deterministic_alert_id(RUN_ID, "col_b", "null_rate")
        assert id1 != id2

    def test_different_metrics_produce_different_ids(self) -> None:
        id1 = _deterministic_alert_id(RUN_ID, "amount", "null_rate")
        id2 = _deterministic_alert_id(RUN_ID, "amount", "distribution_psi")
        assert id1 != id2


class TestAlertsWrittenToJson:
    def test_alerts_written_to_json(self, tmp_path) -> None:
        """generate_alerts must create a valid JSON file at the given path."""
        output_path = str(tmp_path / "alerts.json")
        dr = _make_drift()
        generate_alerts([dr], run_id=RUN_ID, output_path=output_path)
        assert os.path.exists(output_path)
        with open(output_path, "r") as fh:
            data = json.load(fh)
        assert isinstance(data, list)
        assert len(data) > 0

    def test_alerts_json_has_required_keys(self, tmp_path) -> None:
        """Each alert in JSON must have all required keys."""
        output_path = str(tmp_path / "alerts.json")
        dr = _make_drift()
        generate_alerts([dr], run_id=RUN_ID, output_path=output_path)
        with open(output_path, "r") as fh:
            alerts = json.load(fh)
        required_keys = {
            "alert_id", "run_id", "column", "metric", "severity",
            "baseline_value", "current_value", "delta",
            "explanation", "threshold", "triggered_at"
        }
        for alert in alerts:
            missing = required_keys - set(alert.keys())
            assert not missing, f"Alert missing keys: {missing}"

    def test_load_alerts_returns_list(self, tmp_path) -> None:
        output_path = str(tmp_path / "alerts.json")
        dr = _make_drift()
        generate_alerts([dr], run_id=RUN_ID, output_path=output_path)
        loaded = load_alerts(output_path)
        assert isinstance(loaded, list)

    def test_load_alerts_returns_empty_list_if_file_missing(self, tmp_path) -> None:
        missing_path = str(tmp_path / "no_alerts.json")
        result = load_alerts(missing_path)
        assert result == []

    def test_multiple_alerts_all_written(self, tmp_path) -> None:
        output_path = str(tmp_path / "alerts.json")
        drs = [
            _make_drift(column="is_flagged", metric="null_rate"),
            _make_drift(column="amount", metric="distribution_psi", severity="CRITICAL"),
            _make_drift(column="_volume", metric="volume", severity="WARN"),
        ]
        alerts = generate_alerts(drs, run_id=RUN_ID, output_path=output_path)
        assert len(alerts) == 3
        with open(output_path) as fh:
            saved = json.load(fh)
        assert len(saved) == 3
