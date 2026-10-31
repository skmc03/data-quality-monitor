from __future__ import annotations

from typing import Optional

import duckdb

from pipeline.logger import get_logger, _DDL_PIPELINE_RUNS


STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"


def check_run_exists(db_path: str, run_id: str) -> Optional[str]:
    with duckdb.connect(db_path) as con:
        con.execute(_DDL_PIPELINE_RUNS)
        row = con.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = ?", [run_id]
        ).fetchone()
    return row[0] if row else None


def is_already_completed(db_path: str, run_id: str) -> bool:
    return check_run_exists(db_path, run_id) == STATUS_COMPLETED


def purge_run(db_path: str, run_id: str, log_level: str = "INFO") -> None:
    logger = get_logger(__name__, run_id=run_id, level=log_level)
    logger.info("Force-purging existing data for run_id=%s", run_id)

    with duckdb.connect(db_path) as con:
        con.execute(_DDL_PIPELINE_RUNS)
        for table in ("pipeline_runs", "column_profiles", "quarantine"):
            try:
                con.execute(f"DELETE FROM {table} WHERE run_id = ?", [run_id])
                logger.info("Purged %s for run_id=%s", table, run_id)
            except Exception as exc:
                logger.debug("Could not purge %s: %s", table, exc)


def guard_idempotent(
    db_path: str,
    run_id: str,
    *,
    force: bool = False,
    log_level: str = "INFO",
) -> bool:
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
            "run_id=%s previously FAILED — retrying automatically.",
            run_id,
        )
        purge_run(db_path, run_id, log_level=log_level)
        return True

    logger.warning("run_id=%s has unknown status '%s' — proceeding.", run_id, status)
    return True
