"""
run_pipeline.py — Main CLI entrypoint for the Data Quality Monitor.

Usage
-----
python run_pipeline.py \\
    --baseline data/sample_baseline.csv \\
    --batch    data/sample_batch.csv \\
    --run-id   RUN_001

Options
-------
--baseline     Path to baseline CSV (required on first run)
--batch        Path to new batch CSV (required)
--run-id       Unique identifier for this run  [default: auto-generated]
--config       Path to config.yaml             [default: config.yaml]
--db           Override DuckDB path
--force        Re-run even if run_id already completed
--baseline-run-id  Explicitly name the baseline run_id to compare against
--log-level    INFO | DEBUG | WARNING           [default: INFO]
--skip-reconcile   Skip reconciliation step

Exit codes
----------
0 — Pipeline completed successfully
1 — Pipeline failed (exception or critical error)
2 — Pipeline skipped (idempotency; already completed)
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import yaml

# ---------------------------------------------------------------------------
# Pipeline imports
# ---------------------------------------------------------------------------
from pipeline.alerts import generate_alerts
from pipeline.comparator import compare
from pipeline.idempotency import guard_idempotent
from pipeline.ingest import load_csv
from pipeline.logger import RunTracker, get_logger
from pipeline.profiler import (
    get_latest_baseline_run_id,
    load_profile,
    profile,
)
from pipeline.quarantine import quarantine_records
from pipeline.validate import validate
from reconciliation.reconcile import run_reconciliation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config(config_path: str) -> Dict[str, Any]:
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _init_db(db_path: str) -> None:
    """Ensure DuckDB file and parent directory exist."""
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    # DuckDB creates the file on first connect; nothing more needed here.


def _print_summary(
    run_id: str,
    input_count: int,
    valid_count: int,
    rejected_count: int,
    alerts: list,
    elapsed: float,
) -> None:
    divider = "=" * 60
    print(f"\n{divider}")
    print(f"  DATA QUALITY MONITOR — RUN SUMMARY")
    print(divider)
    print(f"  Run ID          : {run_id}")
    print(f"  Elapsed         : {elapsed:.1f}s")
    print(f"  Input rows      : {input_count:,}")
    print(f"  Valid rows      : {valid_count:,}")
    print(f"  Rejected rows   : {rejected_count:,}")
    print(f"  Alerts total    : {len(alerts)}")
    by_sev: Dict[str, int] = {}
    for a in alerts:
        by_sev[a["severity"]] = by_sev.get(a["severity"], 0) + 1
    for sev in ("CRITICAL", "WARN", "INFO"):
        count = by_sev.get(sev, 0)
        if count:
            print(f"    {sev:10s}: {count}")
    print(divider + "\n")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    baseline_path: str,
    batch_path: str,
    run_id: str,
    config: Dict[str, Any],
    *,
    force: bool = False,
    baseline_run_id: Optional[str] = None,
    log_level: str = "INFO",
    skip_reconcile: bool = False,
) -> int:
    """
    Execute the full data quality pipeline.

    Returns exit code (0=success, 1=failure, 2=skipped).
    """
    db_path = config.get("database", {}).get("path", "db/pipeline.duckdb")
    schema_path = config.get("data", {}).get("schema_path", "data/schema.json")
    thresholds = config.get("thresholds", {})
    output = config.get("output", {})

    alerts_path = output.get("alerts_path", "outputs/alerts.json")
    quarantine_csv_path = output.get("quarantine_path", "outputs/quarantine.csv")

    logger = get_logger("run_pipeline", run_id=run_id, level=log_level)

    # ---- Step 0: Initialise DB -------------------------------------------
    _init_db(db_path)
    logger.info("Database initialised at %s", db_path)

    # ---- Step 1: Idempotency guard ---------------------------------------
    should_proceed = guard_idempotent(db_path, run_id, force=force, log_level=log_level)
    if not should_proceed:
        print(f"[SKIP] run_id={run_id} already COMPLETED. Use --force to reprocess.")
        return 2

    # ---- Step 2: Register run start --------------------------------------
    tracker = RunTracker(db_path, run_id)
    tracker.start(source_file=os.path.basename(batch_path))

    try:
        # ---- Step 3: Ingest baseline ------------------------------------
        logger.info("Ingesting baseline: %s", baseline_path)
        baseline_df_raw = load_csv(baseline_path, run_id=f"{run_id}_BASELINE", log_level=log_level)
        logger.info("Baseline raw rows: %d", len(baseline_df_raw))

        # ---- Step 4: Ingest batch ----------------------------------------
        logger.info("Ingesting batch: %s", batch_path)
        batch_df_raw = load_csv(batch_path, run_id=run_id, log_level=log_level)
        input_count = len(batch_df_raw)
        logger.info("Batch raw rows: %d", input_count)
        tracker.start(source_file=os.path.basename(batch_path), input_count=input_count)

        # ---- Step 5: Validate baseline -----------------------------------
        logger.info("Validating baseline data...")
        baseline_valid, baseline_rejected = validate(
            baseline_df_raw, run_id=f"{run_id}_BASELINE",
            schema_path=schema_path, log_level=log_level
        )
        logger.info(
            "Baseline: %d valid, %d rejected", len(baseline_valid), len(baseline_rejected)
        )

        # Quarantine baseline rejects (separate run_id suffix)
        if not baseline_rejected.empty:
            quarantine_records(
                baseline_rejected, db_path,
                run_id=f"{run_id}_BASELINE",
                quarantine_csv_path=quarantine_csv_path,
                log_level=log_level,
            )

        # ---- Step 6: Validate batch --------------------------------------
        logger.info("Validating batch data...")
        batch_valid, batch_rejected = validate(
            batch_df_raw, run_id=run_id,
            schema_path=schema_path, log_level=log_level
        )
        logger.info(
            "Batch: %d valid, %d rejected", len(batch_valid), len(batch_rejected)
        )

        # ---- Step 7: Quarantine batch rejects ----------------------------
        rejected_count = len(batch_rejected)
        if not batch_rejected.empty:
            quarantine_records(
                batch_rejected, db_path,
                run_id=run_id,
                quarantine_csv_path=quarantine_csv_path,
                log_level=log_level,
            )

        # ---- Step 8: Profile baseline ------------------------------------
        logger.info("Profiling baseline...")
        baseline_profile = profile(
            baseline_valid,
            run_id=f"{run_id}_BASELINE",
            db_path=db_path,
            log_level=log_level,
        )

        # ---- Step 9: Profile batch ---------------------------------------
        logger.info("Profiling batch...")
        batch_profile = profile(
            batch_valid,
            run_id=run_id,
            db_path=db_path,
            log_level=log_level,
        )

        # ---- Step 10: Compare batch vs baseline -------------------------
        logger.info("Comparing batch profile vs baseline...")
        drift_results = compare(
            baseline_profile=baseline_profile,
            batch_profile=batch_profile,
            batch_df=batch_valid if not batch_valid.empty else None,
            baseline_df=baseline_valid if not baseline_valid.empty else None,
            run_id=run_id,
            null_rate_delta_threshold=thresholds.get("null_rate_delta", 0.05),
            volume_delta_pct_threshold=thresholds.get("volume_delta_pct", 0.20),
            psi_threshold=thresholds.get("distribution_psi_threshold", 0.2),
            freshness_max_lag_hours=thresholds.get("freshness_max_lag_hours", 48),
            log_level=log_level,
        )

        # ---- Step 11: Generate alerts ------------------------------------
        logger.info("Generating alerts...")
        alerts = generate_alerts(
            drift_results,
            run_id=run_id,
            output_path=alerts_path,
            log_level=log_level,
        )

        # ---- Step 12: Reconciliation ------------------------------------
        if not skip_reconcile:
            logger.info("Running reconciliation checks...")
            _run_reconciliation(
                db_path=db_path,
                run_id=run_id,
                input_count=input_count,
                valid_count=len(batch_valid),
                rejected_count=rejected_count,
                config=config,
                log_level=log_level,
            )

        # ---- Step 13: Mark run complete ----------------------------------
        valid_count = len(batch_valid)
        tracker.complete(output_count=valid_count, rejected_count=rejected_count)

        # ---- Print summary -----------------------------------------------
        _print_summary(
            run_id=run_id,
            input_count=input_count,
            valid_count=valid_count,
            rejected_count=rejected_count,
            alerts=alerts,
            elapsed=tracker.elapsed_seconds(),
        )

        return 0

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        logger.debug(traceback.format_exc())
        tracker.fail(reason=str(exc))
        print(f"\n[FAILED] run_id={run_id}: {exc}", file=sys.stderr)
        return 1


def _run_reconciliation(
    db_path: str,
    run_id: str,
    input_count: int,
    valid_count: int,
    rejected_count: int,
    config: Dict[str, Any],
    log_level: str,
) -> None:
    """Invoke the reconciliation module."""
    try:
        run_reconciliation(
            db_path=db_path,
            run_id=run_id,
            input_count=input_count,
            valid_count=valid_count,
            rejected_count=rejected_count,
            config=config,
            log_level=log_level,
        )
    except Exception as exc:
        logger = get_logger("reconcile", run_id=run_id, level=log_level)
        logger.warning("Reconciliation step raised an error: %s", exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Data Quality Monitor — pipeline entrypoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Path to baseline CSV (uses config.yaml default if omitted)",
    )
    parser.add_argument(
        "--batch",
        default=None,
        help="Path to batch CSV (uses config.yaml default if omitted)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        dest="run_id",
        help="Unique run identifier (auto-generated if omitted)",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml [default: config.yaml]",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Override DuckDB path from config",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess even if run_id already completed",
    )
    parser.add_argument(
        "--baseline-run-id",
        default=None,
        dest="baseline_run_id",
        help="Explicitly specify which run_id to use as baseline for comparison",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        dest="log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity [default: INFO]",
    )
    parser.add_argument(
        "--skip-reconcile",
        action="store_true",
        dest="skip_reconcile",
        help="Skip reconciliation step",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)

    # Load config
    config = _load_config(args.config)

    # Override db path if provided
    if args.db:
        config.setdefault("database", {})["path"] = args.db

    # Resolve baseline and batch paths
    baseline_path = args.baseline or config.get("data", {}).get(
        "baseline_path", "data/sample_baseline.csv"
    )
    batch_path = args.batch or config.get("data", {}).get(
        "batch_path", "data/sample_batch.csv"
    )

    # Generate run_id if not provided
    run_id = args.run_id or f"RUN_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6].upper()}"

    print(f"[START] Data Quality Monitor | run_id={run_id}")
    print(f"        Baseline : {baseline_path}")
    print(f"        Batch    : {batch_path}")

    exit_code = run_pipeline(
        baseline_path=baseline_path,
        batch_path=batch_path,
        run_id=run_id,
        config=config,
        force=args.force,
        baseline_run_id=args.baseline_run_id,
        log_level=args.log_level,
        skip_reconcile=args.skip_reconcile,
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
