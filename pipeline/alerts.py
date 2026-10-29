"""
Alerts module — convert DriftResults into explainable, structured alert reports.

Each alert has:
    alert_id           : deterministic SHA-256 hash of (run_id, column, metric)
    run_id             : pipeline run
    column             : affected column
    metric             : null_rate | volume | distribution_psi | freshness | new_category | missing_category
    severity           : INFO | WARN | CRITICAL
    baseline_value     : value in baseline profile
    current_value      : value in current batch profile
    delta              : numeric change magnitude
    explanation        : human-readable explanation (>= 1 sentence)
    threshold          : configured threshold that was crossed
    triggered_at       : ISO-8601 UTC timestamp

Written to outputs/alerts.json.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipeline.comparator import DriftResult
from pipeline.logger import get_logger


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_alerts(
    drift_results: List[DriftResult],
    run_id: str,
    *,
    output_path: str = "outputs/alerts.json",
    log_level: str = "INFO",
    min_severity: str = "INFO",
) -> List[Dict[str, Any]]:
    """
    Convert DriftResult objects into structured alert dictionaries and write
    them to a JSON file.

    Parameters
    ----------
    drift_results:  List of DriftResult instances from the comparator.
    run_id:         Pipeline run identifier.
    output_path:    Where to write alerts.json.
    log_level:      Logging level.
    min_severity:   Minimum severity to include ('INFO', 'WARN', 'CRITICAL').

    Returns
    -------
    List of alert dicts (all severities included in return; file may filter).
    """
    logger = get_logger(__name__, run_id=run_id, level=log_level)
    severity_rank = {"INFO": 0, "WARN": 1, "CRITICAL": 2}
    min_rank = severity_rank.get(min_severity.upper(), 0)

    triggered_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    alerts: List[Dict[str, Any]] = []

    for dr in drift_results:
        alert_id = _deterministic_alert_id(run_id, dr.column, dr.metric)
        explanation = _build_explanation(dr)

        alert: Dict[str, Any] = {
            "alert_id": alert_id,
            "run_id": run_id,
            "column": dr.column,
            "metric": dr.metric,
            "severity": dr.severity,
            "baseline_value": _safe_serialize(dr.baseline_value),
            "current_value": _safe_serialize(dr.current_value),
            "delta": _safe_serialize(dr.delta),
            "explanation": explanation,
            "threshold": _safe_serialize(dr.threshold),
            "triggered_at": triggered_at,
        }
        alerts.append(alert)

    # Filter by min severity for file output
    filtered = [a for a in alerts if severity_rank.get(a["severity"], 0) >= min_rank]

    # Write to JSON
    _write_alerts_json(alerts, output_path, logger)  # Write all; consumer can filter

    logger.info(
        "Generated %d alerts (%d CRITICAL, %d WARN, %d INFO) for run_id=%s",
        len(alerts),
        sum(1 for a in alerts if a["severity"] == "CRITICAL"),
        sum(1 for a in alerts if a["severity"] == "WARN"),
        sum(1 for a in alerts if a["severity"] == "INFO"),
        run_id,
    )

    return alerts


def load_alerts(output_path: str) -> List[Dict[str, Any]]:
    """Load previously written alerts from JSON file."""
    if not os.path.exists(output_path):
        return []
    with open(output_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Explanation builder
# ---------------------------------------------------------------------------

def _build_explanation(dr: DriftResult) -> str:
    """
    Generate a human-readable explanation for a drift result.

    The explanation goes beyond just repeating numbers — it describes
    the business impact and what the analyst should investigate.
    """
    metric = dr.metric
    col = dr.column
    sev = dr.severity

    if metric == "null_rate":
        delta_pp = dr.delta * 100
        direction = "risen" if dr.delta > 0 else "fallen"
        base_pct = (dr.baseline_value or 0) * 100
        curr_pct = (dr.current_value or 0) * 100
        explanation = (
            f"The null rate for column '{col}' has {direction} by {abs(delta_pp):.2f} percentage points "
            f"(from {base_pct:.2f}% to {curr_pct:.2f}%). "
        )
        if sev == "CRITICAL":
            explanation += (
                "This is a large increase and likely indicates an upstream data collection failure, "
                "a schema change in the source system, or a broken ETL step that no longer populates this field. "
                "Immediate investigation recommended."
            )
        elif sev == "WARN":
            explanation += (
                "This may indicate data quality degradation or a partial upstream issue. "
                "Review source system logs and upstream pipeline health."
            )
        else:
            explanation += "Monitor over the next few batches to determine if this is a trend."

    elif metric == "volume":
        pct_change = dr.delta * 100
        direction = "increased" if dr.delta > 0 else "decreased"
        explanation = (
            f"Row count has {direction} by {abs(pct_change):.1f}% "
            f"(baseline={dr.baseline_value}, current={dr.current_value}). "
        )
        if sev == "CRITICAL":
            explanation += (
                "A volume change of this magnitude is unusual and warrants investigation. "
                "Possible causes: truncated batch, duplicate injection, source system outage, "
                "or backfill event. Verify ingestion completeness."
            )
        else:
            explanation += (
                "This may be expected for a growing or shrinking business event. "
                "Confirm with the data producer whether this is intentional."
            )

    elif metric == "distribution_psi":
        psi = dr.delta
        base_mean = dr.baseline_value
        curr_mean = dr.current_value
        explanation = (
            f"The distribution of '{col}' has shifted significantly. "
            f"Population Stability Index (PSI) = {psi:.4f} (threshold={dr.threshold}). "
            f"Baseline mean was {base_mean}, current mean is {curr_mean}. "
        )
        if sev == "CRITICAL":
            explanation += (
                "PSI above 0.25 indicates a major population shift. "
                "The data generating process may have fundamentally changed. "
                "Inspect for pricing changes, new customer segments, currency shifts, or data errors."
            )
        else:
            explanation += (
                "PSI between 0.1 and 0.25 suggests a moderate shift. "
                "Review business context (e.g., seasonal effects, new products) "
                "before treating this as an error."
            )

    elif metric == "freshness":
        lag_hours = dr.current_value
        threshold_hours = dr.threshold
        explanation = (
            f"The most recent transaction_date in this batch is {lag_hours:.1f} hours old, "
            f"exceeding the freshness threshold of {threshold_hours}h. "
        )
        if sev == "CRITICAL":
            explanation += (
                "This strongly suggests a pipeline delay, source system outage, or missed ingestion job. "
                "Check the batch scheduler, source database connectivity, and any upstream SLA breaches."
            )
        else:
            explanation += (
                "The data may be slightly delayed. Verify whether the source system had a processing delay "
                "or whether the batch window was shifted."
            )

    elif metric == "new_category":
        explanation = (
            f"A new category value '{dr.current_value}' appeared in column '{col}' "
            f"that was not present in the baseline dataset. "
            "This could indicate a new valid category being added to the source system, "
            "a data entry error, or schema drift from the upstream producer. "
            "Validate against the official data dictionary and update the allowed-values list if intentional."
        )

    elif metric == "missing_category":
        explanation = (
            f"Category value '{dr.baseline_value}' from column '{col}' is absent in the current batch. "
            "This was present in the baseline dataset. "
            "Possible causes: the category is no longer used, transactions of this type were not included "
            "in the batch window, or records were filtered upstream. "
            "Confirm with the data producer if this is expected."
        )

    else:
        explanation = dr.description or f"Drift detected on column '{col}', metric '{metric}'."

    return explanation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deterministic_alert_id(run_id: str, column: str, metric: str) -> str:
    """SHA-256 hash of (run_id, column, metric) encoded as hex string."""
    key = f"{run_id}::{column}::{metric}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _safe_serialize(value: Any) -> Any:
    """Convert numpy/pandas types to native Python for JSON serialisation."""
    if value is None:
        return None
    try:
        import numpy as np
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        if isinstance(value, np.ndarray):
            return value.tolist()
    except ImportError:
        pass
    return value


def _write_alerts_json(alerts: List[Dict[str, Any]], output_path: str, logger) -> None:
    """Write alert list to JSON file, creating parent directories if needed."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(alerts, fh, indent=2, default=str)
    logger.info("Alerts written to %s (%d records)", output_path, len(alerts))
