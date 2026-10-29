"""
Idempotency module — ensure pipeline runs can be safely re-executed.

Behaviour
---------
- On first run:  register run_id, proceed normally.
- On rerun:      detect existing run_id, skip processing, return cached results.
- With --force:  delete previous state for run_id and reprocess from scratch.

Checks pipeline_runs table (managed by logger.RunTracker) for run status.
"""

from __future__ import annotations

from typing import Optional

import duckdb

from pipeline.logger import get_logger, _DDL_PIPELINE_RUNS


# ---------------------------------------------------------------------------
# Run status constants
# ---------------------------------------------------------------------------

STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_run_exists(db_path: str, run_id: str) -> Optional[str]:
    """
    Check if a run_id already exists in pipeline_runs.

    Parameters
    ----------
    db_path:  Path to DuckDB file.
    run_id:   Run identifier to look up.

    Returns
    -------
    Run status string ('RUNNING', 'COMPLETED', 'FAILED') or None if not found.
    """
    with duckdb.connect(db_path) as con:
        con.execute(_DDL_PIPELINE_RUNS)
        row = con.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = ?", [run_id]
        ).fetchone()
    return row[0] if row else None


def is_already_completed(db_path: str, run_id: str) -> bool:
    """Return True if run_id has status COMPLETED."""
    status = check_run_exists(db_path, run_id)
    return status == STATUS_COMPLETED


def purge_run(db_path: str, run_id: str, log_level: str = "INFO") -> None:
    """
    Remove all data associated with a run_id from DuckDB tables.

    Tables cleaned:
        - pipeline_runs
        - column_profiles
        - quarantine

    Used when --force flag is provided.

    Parameters
    ----------
    db_path:   Path to DuckDB file.
    run_id:    Run to purge.
    log_level: Logging level.
    """
    logger = get_logger(__name__, run_id=run_id, level=log_level)
    logger.info("Force-purging existing data for run_id=%s", run_id)

    with duckdb.connect(db_path) as con:
        # Ensure tables exist before attempting deletes
        con.execute(_DDL_PIPELINE_RUNS)

        for table in ("pipeline_runs", "column_profiles", "quarantine"):
            try:
                con.execute(f"DELETE FROM {table} WHERE run_id = ?", [run_id])
                logger.info("Purged %s for run_id=%s", table, run_id)
            except Exception as exc:
                # Table may not exist yet on first-ever run
                logger.debug("Could not purge %s: %s", table, exc)


def guard_idempotent(
    db_path: str,
    run_id: str,
    *,
    force: bool = False,
    log_level: str = "INFO",
) -> bool:
    """
    Idempotency guard — call at the start of the pipeline.

    Parameters
    ----------
    db_path:   Path to DuckDB file.
    run_id:    Run identifier.
    force:     If True, purge existing run data and reprocess.
    log_level: Logging level.

    Returns
    -------
    True  — run should proceed (first run or forced).
    False — run should be skipped (already completed).
    """
    logger = get_logger(__name__, run_id=run_id, level=log_level)

    status = check_run_exists(db_path, run_id)

    if status is None:
        logger.info("run_id=%s is new — proceeding.", run_id)
        return True

    if force:
        logger.warning(
            "run_id=%s already exists (status=%s) but --force flag set — purging and reprocessing.",
            run_id,
            status,
        )
        purge_run(db_path, run_id, log_level=log_level)
        return True

    if status == STATUS_COMPLETED:
        logger.info(
            "run_id=%s already COMPLETED — skipping. Use --force to reprocess.",
            run_id,
        )
        return False

    if status == STATUS_RUNNING:
        logger.warning(
            "run_id=%s is currently RUNNING (possibly from a crashed prior attempt). "
            "If this is stale, use --force to reprocess.",
            run_id,
        )
        return False

    if status == STATUS_FAILED:
        logger.info(
            "run_id=%s previously FAILED — retrying (automatically allows rerun after failure).",
            run_id,
        )
        purge_run(db_path, run_id, log_level=log_level)
        return True

    # Unknown status — proceed with caution
    logger.warning("run_id=%s has unknown status '%s' — proceeding.", run_id, status)
    return True
