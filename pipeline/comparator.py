"""
Comparator module — compare a new batch profile vs a baseline profile.

Metrics computed
----------------
- Null rate delta per column          (WARN if > threshold)
- Volume change %                     (WARN/CRITICAL)
- Distribution drift: PSI for numeric (WARN if > 0.2, CRITICAL if > 0.25)
- Freshness lag (hours)               (WARN/CRITICAL)
- New/missing categories in categorical columns

Returns a list of DriftResult dataclass instances.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from pipeline.logger import get_logger
from pipeline.profiler import load_profile


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class DriftResult:
    column: str
    metric: str          # null_rate | volume | distribution_psi | freshness | new_category | missing_category
    baseline_value: Any
    current_value: Any
    delta: float
    severity: str        # INFO | WARN | CRITICAL
    description: str     # human-readable detail
    threshold: float = 0.0

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "metric": self.metric,
            "baseline_value": self.baseline_value,
            "current_value": self.current_value,
            "delta": self.delta,
            "severity": self.severity,
            "description": self.description,
            "threshold": self.threshold,
        }


# ---------------------------------------------------------------------------
# PSI implementation
# ---------------------------------------------------------------------------

def _compute_psi(
    baseline_vals: np.ndarray,
    current_vals: np.ndarray,
    n_bins: int = 10,
) -> float:
    """
    Population Stability Index (PSI).

    PSI = sum((actual% - expected%) * ln(actual% / expected%))

    PSI < 0.1  : No significant change
    0.1–0.2    : Moderate change — monitor
    > 0.2      : Significant shift — action needed
    """
    # Build bins from baseline
    _, bin_edges = np.histogram(baseline_vals, bins=n_bins)
    # Extend edges to capture all values
    bin_edges[0] = -np.inf
    bin_edges[-1] = np.inf

    baseline_counts, _ = np.histogram(baseline_vals, bins=bin_edges)
    current_counts, _ = np.histogram(current_vals, bins=bin_edges)

    # Convert to proportions, avoid division by zero
    eps = 1e-6
    baseline_pct = baseline_counts / (baseline_counts.sum() + eps)
    current_pct = current_counts / (current_counts.sum() + eps)
    baseline_pct = np.where(baseline_pct == 0, eps, baseline_pct)
    current_pct = np.where(current_pct == 0, eps, current_pct)

    psi_values = (current_pct - baseline_pct) * np.log(current_pct / baseline_pct)
    return float(np.sum(psi_values))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compare(
    baseline_profile: Dict[str, Any],
    batch_profile: Dict[str, Any],
    batch_df: Optional[pd.DataFrame],
    baseline_df: Optional[pd.DataFrame],
    run_id: str,
    *,
    null_rate_delta_threshold: float = 0.05,
    volume_delta_pct_threshold: float = 0.20,
    psi_threshold: float = 0.2,
    freshness_max_lag_hours: float = 48.0,
    log_level: str = "INFO",
) -> List[DriftResult]:
    """
    Compare batch profile against baseline profile and return drift results.

    Parameters
    ----------
    baseline_profile:  Profile dict for the baseline run.
    batch_profile:     Profile dict for the current batch run.
    batch_df:          Raw batch DataFrame (needed for PSI histogram computation).
    baseline_df:       Raw baseline DataFrame (needed for PSI histogram computation).
    run_id:            Current batch run_id.
    *threshold params* as per config.yaml.

    Returns
    -------
    List of DriftResult instances (may be empty if no drift detected).
    """
    logger = get_logger(__name__, run_id=run_id, level=log_level)
    results: List[DriftResult] = []

    if not baseline_profile:
        logger.warning("Baseline profile is empty — cannot compare.")
        return results

    # ---- 1. Volume --------------------------------------------------------
    baseline_volume = baseline_profile.get("_volume", {}).get("row_count", 0)
    batch_volume = batch_profile.get("_volume", {}).get("row_count", 0)

    if baseline_volume > 0:
        volume_delta = (batch_volume - baseline_volume) / baseline_volume
        abs_delta = abs(volume_delta)
        if abs_delta >= volume_delta_pct_threshold:
            severity = "CRITICAL" if abs_delta >= 2 * volume_delta_pct_threshold else "WARN"
            direction = "increased" if volume_delta > 0 else "decreased"
            results.append(
                DriftResult(
                    column="_volume",
                    metric="volume",
                    baseline_value=baseline_volume,
                    current_value=batch_volume,
                    delta=round(volume_delta, 4),
                    severity=severity,
                    threshold=volume_delta_pct_threshold,
                    description=(
                        f"Row count {direction} by {abs_delta * 100:.1f}% "
                        f"(baseline={baseline_volume}, current={batch_volume})"
                    ),
                )
            )

    # ---- 2. Null rate delta per column ------------------------------------
    all_cols = set(baseline_profile.keys()) | set(batch_profile.keys())
    all_cols.discard("_volume")

    for col in sorted(all_cols):
        b_nr = baseline_profile.get(col, {}).get("null_rate")
        c_nr = batch_profile.get(col, {}).get("null_rate")
        if b_nr is None or c_nr is None:
            continue
        delta = c_nr - b_nr
        abs_delta = abs(delta)
        if abs_delta >= null_rate_delta_threshold:
            severity = "CRITICAL" if abs_delta >= 3 * null_rate_delta_threshold else "WARN"
            direction = "increased" if delta > 0 else "decreased"
            results.append(
                DriftResult(
                    column=col,
                    metric="null_rate",
                    baseline_value=round(b_nr, 4),
                    current_value=round(c_nr, 4),
                    delta=round(delta, 4),
                    severity=severity,
                    threshold=null_rate_delta_threshold,
                    description=(
                        f"Null rate for '{col}' {direction} by {abs_delta * 100:.2f}pp "
                        f"(baseline={b_nr * 100:.2f}%, current={c_nr * 100:.2f}%)"
                    ),
                )
            )

    # ---- 3. Distribution PSI for numeric columns -------------------------
    numeric_cols_in_baseline = [
        col for col, p in baseline_profile.items()
        if p.get("profile_type") == "numeric" and col != "_volume"
    ]

    for col in numeric_cols_in_baseline:
        if batch_df is None or baseline_df is None:
            break
        if col not in batch_df.columns or col not in baseline_df.columns:
            continue

        base_vals = pd.to_numeric(baseline_df[col], errors="coerce").dropna().values
        curr_vals = pd.to_numeric(batch_df[col], errors="coerce").dropna().values

        if len(base_vals) < 5 or len(curr_vals) < 5:
            continue

        psi = _compute_psi(base_vals, curr_vals)

        if psi >= psi_threshold:
            severity = "CRITICAL" if psi >= psi_threshold * 1.5 else "WARN"
            results.append(
                DriftResult(
                    column=col,
                    metric="distribution_psi",
                    baseline_value=round(float(np.mean(base_vals)), 2),
                    current_value=round(float(np.mean(curr_vals)), 2),
                    delta=round(psi, 4),
                    severity=severity,
                    threshold=psi_threshold,
                    description=(
                        f"Distribution of '{col}' has significantly shifted. "
                        f"PSI={psi:.4f} (threshold={psi_threshold}). "
                        f"Baseline mean={np.mean(base_vals):.2f}, "
                        f"current mean={np.mean(curr_vals):.2f}."
                    ),
                )
            )
        else:
            results.append(
                DriftResult(
                    column=col,
                    metric="distribution_psi",
                    baseline_value=round(float(np.mean(base_vals)), 2),
                    current_value=round(float(np.mean(curr_vals)), 2),
                    delta=round(psi, 4),
                    severity="INFO",
                    threshold=psi_threshold,
                    description=(
                        f"Distribution of '{col}' is stable. PSI={psi:.4f} (threshold={psi_threshold})."
                    ),
                )
            )

    # ---- 4. Freshness lag -------------------------------------------------
    batch_freshness = batch_profile.get("transaction_date", {})
    if batch_freshness.get("profile_type") == "freshness":
        freshness_max_str = batch_freshness.get("freshness_max")
        if freshness_max_str:
            try:
                latest_ts = pd.to_datetime(freshness_max_str, utc=True)
                now_utc = datetime.now(timezone.utc)
                lag_hours = (now_utc - latest_ts.to_pydatetime()).total_seconds() / 3600
                if lag_hours > freshness_max_lag_hours:
                    severity = "CRITICAL" if lag_hours > 2 * freshness_max_lag_hours else "WARN"
                    results.append(
                        DriftResult(
                            column="transaction_date",
                            metric="freshness",
                            baseline_value=str(freshness_max_lag_hours) + "h",
                            current_value=round(lag_hours, 2),
                            delta=round(lag_hours - freshness_max_lag_hours, 2),
                            severity=severity,
                            threshold=freshness_max_lag_hours,
                            description=(
                                f"Most recent transaction_date is {lag_hours:.1f} hours ago "
                                f"(latest={freshness_max_str}). "
                                f"Threshold is {freshness_max_lag_hours}h. "
                                f"Data may be stale or a pipeline delay occurred."
                            ),
                        )
                    )
            except Exception as e:
                logger.warning("Could not compute freshness lag: %s", e)

    # ---- 5. New / missing categories in categorical columns ---------------
    cat_cols_in_baseline = [
        col for col, p in baseline_profile.items()
        if p.get("profile_type") == "categorical" and col != "_volume"
    ]

    for col in cat_cols_in_baseline:
        baseline_cats = set(baseline_profile.get(col, {}).get("value_counts", {}).keys())
        current_cats = set(batch_profile.get(col, {}).get("value_counts", {}).keys())

        new_cats = current_cats - baseline_cats
        missing_cats = baseline_cats - current_cats

        for cat in sorted(new_cats):
            pct = batch_profile.get(col, {}).get("value_counts", {}).get(cat, 0) * 100
            results.append(
                DriftResult(
                    column=col,
                    metric="new_category",
                    baseline_value=None,
                    current_value=cat,
                    delta=0.0,
                    severity="WARN",
                    threshold=0.0,
                    description=(
                        f"New category '{cat}' appeared in column '{col}' "
                        f"(not present in baseline). Current proportion: {pct:.2f}%."
                    ),
                )
            )

        for cat in sorted(missing_cats):
            pct = baseline_profile.get(col, {}).get("value_counts", {}).get(cat, 0) * 100
            results.append(
                DriftResult(
                    column=col,
                    metric="missing_category",
                    baseline_value=cat,
                    current_value=None,
                    delta=0.0,
                    severity="INFO",
                    threshold=0.0,
                    description=(
                        f"Category '{cat}' from column '{col}' is absent in current batch "
                        f"(baseline proportion was {pct:.2f}%)."
                    ),
                )
            )

    logger.info(
        "Comparison complete: %d drift results (%d CRITICAL, %d WARN, %d INFO)",
        len(results),
        sum(1 for r in results if r.severity == "CRITICAL"),
        sum(1 for r in results if r.severity == "WARN"),
        sum(1 for r in results if r.severity == "INFO"),
    )
    return results
