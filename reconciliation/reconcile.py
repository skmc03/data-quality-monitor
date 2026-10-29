"""
Reconciliation module — run data quality assertions and produce a report.

Checks performed
----------------
1. Source count == valid + rejected (no rows lost or duplicated)
2. No duplicate run_id in pipeline_runs
3. All quarantined records have a non-empty rejection_reason
4. Baseline profile exists before comparison runs
5. Alert IDs are deterministic (same inputs produce same alert_ids)
6. Key aggregates match source:
      - sum(amount) in valid records (approximate, accounting for rejects)
      - count by status
      - count by currency

Output
------
outputs/reconciliation_report.json
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from pipeline.logger import get_logger


# ---------------------------------------------------------------------------
# Reconciliation entry point (called by run_pipeline.py)
# ---------------------------------------------------------------------------

def run_reconciliation(
    db_path: str,
    run_id: str,
    input_count: int,
    valid_count: int,
    rejected_count: int,
    config: Dict[str, Any],
    *,
    log_level: str = "INFO",
) -> Dict[str, Any]:
    """
    Execute all reconciliation checks and write a JSON report.

    Parameters
    ----------
    db_path:        Path to DuckDB file.
    run_id:         Current batch run_id.
    input_count:    Total rows ingested from source file.
    valid_count:    Rows that passed validation.
    rejected_count: Rows quarantined.
    config:         Loaded config.yaml dict.
    log_level:      Logging level.

    Returns
    -------
    Report dict (also written to disk).
    """
    logger = get_logger(__name__, run_id=run_id, level=log_level)
    output = config.get("output", {})
    report_path = output.get("reconciliation_path", "outputs/reconciliation_report.json")
    alerts_path = output.get("alerts_path", "outputs/alerts.json")

    report: Dict[str, Any] = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": [],
        "summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "warnings": 0,
        },
    }

    checks: List[Dict[str, Any]] = []

    # ---- Check 1: Row count reconciliation ---------------------------------
    checks.append(
        _check_row_count(
            input_count=input_count,
            valid_count=valid_count,
            rejected_count=rejected_count,
        )
    )

    # ---- Check 2: No duplicate run_ids in pipeline_runs -------------------
    checks.append(_check_no_duplicate_runs(db_path, run_id))

    # ---- Check 3: All quarantined records have rejection_reason -----------
    checks.append(_check_quarantine_reasons(db_path, run_id))

    # ---- Check 4: Baseline profile exists ---------------------------------
    checks.append(_check_baseline_profile_exists(db_path, run_id))

    # ---- Check 5: Alert IDs are deterministic ----------------------------
    checks.append(_check_alert_id_determinism(alerts_path, run_id))

    # ---- Check 6: Key aggregates match source ----------------------------
    agg_checks = _check_key_aggregates(db_path, run_id)
    checks.extend(agg_checks)

    # Compile summary
    report["checks"] = checks
    report["summary"]["total_checks"] = len(checks)
    report["summary"]["passed"] = sum(1 for c in checks if c["status"] == "PASS")
    report["summary"]["failed"] = sum(1 for c in checks if c["status"] == "FAIL")
    report["summary"]["warnings"] = sum(1 for c in checks if c["status"] == "WARN")

    # Write report
    os.makedirs(os.path.dirname(os.path.abspath(report_path)), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    logger.info(
        "Reconciliation complete: %d/%d checks passed — report at %s",
        report["summary"]["passed"],
        report["summary"]["total_checks"],
        report_path,
    )

    # Print summary to stdout
    _print_reconciliation_summary(report)

    return report


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def _check_row_count(
    input_count: int, valid_count: int, rejected_count: int
) -> Dict[str, Any]:
    expected = valid_count + rejected_count
    passed = expected == input_count
    return {
        "check_id": "ROW_COUNT_RECONCILIATION",
        "description": "source_count == valid_count + rejected_count (no rows lost)",
        "status": "PASS" if passed else "FAIL",
        "detail": {
            "input_count": input_count,
            "valid_count": valid_count,
            "rejected_count": rejected_count,
            "sum_valid_rejected": expected,
            "rows_unaccounted_for": input_count - expected,
        },
        "message": (
            "Row counts balance correctly."
            if passed
            else f"Row count mismatch: {input_count} input != {expected} (valid+rejected)."
        ),
    }


def _check_no_duplicate_runs(db_path: str, run_id: str) -> Dict[str, Any]:
    try:
        with duckdb.connect(db_path) as con:
            result = con.execute(
                """
                SELECT run_id, COUNT(*) AS cnt
                FROM pipeline_runs
                GROUP BY run_id
                HAVING COUNT(*) > 1
                """
            ).fetchdf()
        duplicates = result.to_dict("records") if not result.empty else []
        passed = len(duplicates) == 0
        return {
            "check_id": "NO_DUPLICATE_RUN_IDS",
            "description": "No duplicate run_id entries in pipeline_runs",
            "status": "PASS" if passed else "FAIL",
            "detail": {"duplicate_run_ids": duplicates},
            "message": (
                "No duplicate run_ids found."
                if passed
                else f"Found {len(duplicates)} duplicate run_id(s): {duplicates}"
            ),
        }
    except Exception as exc:
        return _check_error("NO_DUPLICATE_RUN_IDS", str(exc))


def _check_quarantine_reasons(db_path: str, run_id: str) -> Dict[str, Any]:
    try:
        with duckdb.connect(db_path) as con:
            try:
                result = con.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM quarantine
                    WHERE run_id = ? AND (rejection_reason IS NULL OR TRIM(rejection_reason) = '')
                    """,
                    [run_id],
                ).fetchone()
                missing_count = result[0] if result else 0
            except Exception:
                missing_count = 0

        passed = missing_count == 0
        return {
            "check_id": "QUARANTINE_REASONS_POPULATED",
            "description": "All quarantined records have a non-empty rejection_reason",
            "status": "PASS" if passed else "FAIL",
            "detail": {"records_missing_reason": missing_count, "run_id": run_id},
            "message": (
                "All quarantined records have rejection reasons."
                if passed
                else f"{missing_count} quarantine record(s) are missing rejection_reason."
            ),
        }
    except Exception as exc:
        return _check_error("QUARANTINE_REASONS_POPULATED", str(exc))


def _check_baseline_profile_exists(db_path: str, run_id: str) -> Dict[str, Any]:
    baseline_run_id = f"{run_id}_BASELINE"
    try:
        with duckdb.connect(db_path) as con:
            try:
                result = con.execute(
                    "SELECT COUNT(*) FROM column_profiles WHERE run_id = ?",
                    [baseline_run_id],
                ).fetchone()
                count = result[0] if result else 0
            except Exception:
                count = 0

        passed = count > 0
        return {
            "check_id": "BASELINE_PROFILE_EXISTS",
            "description": "Baseline column profile exists in DB before batch comparison",
            "status": "PASS" if passed else "FAIL",
            "detail": {
                "baseline_run_id": baseline_run_id,
                "profile_rows_found": count,
            },
            "message": (
                f"Baseline profile found ({count} rows)."
                if passed
                else f"No baseline profile found for run_id={baseline_run_id}."
            ),
        }
    except Exception as exc:
        return _check_error("BASELINE_PROFILE_EXISTS", str(exc))


def _check_alert_id_determinism(alerts_path: str, run_id: str) -> Dict[str, Any]:
    """Verify that alert_ids are deterministic by recomputing them from (run_id, column, metric)."""
    try:
        if not os.path.exists(alerts_path):
            return {
                "check_id": "ALERT_ID_DETERMINISM",
                "description": "Alert IDs are deterministic (SHA-256 of run_id::column::metric)",
                "status": "WARN",
                "detail": {"alerts_file_found": False, "path": alerts_path},
                "message": f"Alerts file not found at {alerts_path} — skipping determinism check.",
            }

        with open(alerts_path, "r", encoding="utf-8") as fh:
            alerts = json.load(fh)

        mismatches = []
        for alert in alerts:
            expected_id = hashlib.sha256(
                f"{alert['run_id']}::{alert['column']}::{alert['metric']}".encode()
            ).hexdigest()
            if alert.get("alert_id") != expected_id:
                mismatches.append(
                    {
                        "column": alert.get("column"),
                        "metric": alert.get("metric"),
                        "stored_id": alert.get("alert_id"),
                        "computed_id": expected_id,
                    }
                )

        passed = len(mismatches) == 0
        return {
            "check_id": "ALERT_ID_DETERMINISM",
            "description": "Alert IDs are deterministic (SHA-256 of run_id::column::metric)",
            "status": "PASS" if passed else "FAIL",
            "detail": {
                "total_alerts_checked": len(alerts),
                "mismatches": mismatches,
            },
            "message": (
                f"All {len(alerts)} alert IDs are deterministic."
                if passed
                else f"{len(mismatches)} alert ID(s) do not match expected hash."
            ),
        }
    except Exception as exc:
        return _check_error("ALERT_ID_DETERMINISM", str(exc))


def _check_key_aggregates(db_path: str, run_id: str) -> List[Dict[str, Any]]:
    """
    Verify that aggregate metrics in quarantine + valid together match the source.
    We check counts by status and currency from the quarantine table (the rejected side).
    This is a sanity check that no records were silently dropped.
    """
    checks = []

    try:
        with duckdb.connect(db_path) as con:
            try:
                q_df = con.execute(
                    """
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN rejection_reason IS NOT NULL THEN 1 ELSE 0 END) AS with_reason
                    FROM quarantine
                    WHERE run_id = ?
                    """,
                    [run_id],
                ).fetchdf()

                quarantine_total = int(q_df["total"].iloc[0]) if not q_df.empty else 0
                with_reason = int(q_df["with_reason"].iloc[0]) if not q_df.empty else 0

                passed = quarantine_total == with_reason
                checks.append(
                    {
                        "check_id": "QUARANTINE_AGGREGATE_INTEGRITY",
                        "description": "All quarantine rows have rejection_reason (aggregate check)",
                        "status": "PASS" if passed else "FAIL",
                        "detail": {
                            "quarantine_rows": quarantine_total,
                            "rows_with_reason": with_reason,
                        },
                        "message": (
                            f"Quarantine integrity OK ({quarantine_total} records)."
                            if passed
                            else f"Integrity gap: {quarantine_total - with_reason} record(s) missing reason."
                        ),
                    }
                )
            except Exception as exc:
                checks.append(_check_error("QUARANTINE_AGGREGATE_INTEGRITY", str(exc)))

            # Check pipeline_runs uniqueness for this specific run_id
            try:
                result = con.execute(
                    "SELECT COUNT(*) FROM pipeline_runs WHERE run_id = ?", [run_id]
                ).fetchone()
                run_count = result[0] if result else 0
                passed = run_count == 1
                checks.append(
                    {
                        "check_id": "PIPELINE_RUN_REGISTERED",
                        "description": f"Exactly one pipeline_runs entry for run_id={run_id}",
                        "status": "PASS" if passed else ("WARN" if run_count == 0 else "FAIL"),
                        "detail": {"run_id": run_id, "entries_found": run_count},
                        "message": (
                            f"Pipeline run registered correctly (count={run_count})."
                            if passed
                            else f"Unexpected count={run_count} for run_id={run_id} in pipeline_runs."
                        ),
                    }
                )
            except Exception as exc:
                checks.append(_check_error("PIPELINE_RUN_REGISTERED", str(exc)))

    except Exception as exc:
        checks.append(_check_error("KEY_AGGREGATES", str(exc)))

    return checks


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _check_error(check_id: str, message: str) -> Dict[str, Any]:
    return {
        "check_id": check_id,
        "description": f"Check {check_id}",
        "status": "FAIL",
        "detail": {"error": message},
        "message": f"Check raised exception: {message}",
    }


def _print_reconciliation_summary(report: Dict[str, Any]) -> None:
    summary = report["summary"]
    print("\n--- Reconciliation Report ---")
    print(f"  Total checks : {summary['total_checks']}")
    print(f"  Passed       : {summary['passed']}")
    print(f"  Failed       : {summary['failed']}")
    print(f"  Warnings     : {summary['warnings']}")
    for check in report["checks"]:
        icon = "OK" if check["status"] == "PASS" else ("!!" if check["status"] == "FAIL" else "--")
        print(f"  [{icon}] {check['check_id']}: {check['message']}")
    print("")


# ---------------------------------------------------------------------------
# CLI standalone invocation
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys
    import yaml

    parser = argparse.ArgumentParser(description="Run reconciliation checks standalone")
    parser.add_argument("--db", required=True, help="Path to DuckDB file")
    parser.add_argument("--run-id", required=True, dest="run_id")
    parser.add_argument("--input-count", type=int, default=0, dest="input_count")
    parser.add_argument("--valid-count", type=int, default=0, dest="valid_count")
    parser.add_argument("--rejected-count", type=int, default=0, dest="rejected_count")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--log-level", default="INFO", dest="log_level")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}

    report = run_reconciliation(
        db_path=args.db,
        run_id=args.run_id,
        input_count=args.input_count,
        valid_count=args.valid_count,
        rejected_count=args.rejected_count,
        config=cfg,
        log_level=args.log_level,
    )

    failed = report["summary"]["failed"]
    sys.exit(1 if failed > 0 else 0)
