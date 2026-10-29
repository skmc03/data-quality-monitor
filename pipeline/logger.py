"""
Structured logger that injects run_id into every log line.

Tracks:
  run_id, start_time, end_time, input_count, output_count,
  rejected_count, failure_reason

Writes run metadata to DuckDB table: pipeline_runs
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import duckdb


# ---------------------------------------------------------------------------
# Custom log filter that injects run_id into every LogRecord
# ---------------------------------------------------------------------------

class RunIdFilter(logging.Filter):
    def __init__(self, run_id: str = "UNKNOWN"):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def get_logger(name: str, run_id: str = "UNKNOWN", level: str = "INFO") -> logging.Logger:
    """
    Return a logger that stamps every line with run_id.

    Parameters
    ----------
    name:    logger name (typically __name__)
    run_id:  pipeline run identifier
    level:   logging level string
    """
    logger = logging.getLogger(f"{name}.{run_id}")
    if logger.handlers:
        # Already configured for this run; update filter
        for f in logger.filters:
            if isinstance(f, RunIdFilter):
                f.run_id = run_id
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler()
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | run_id=%(run_id)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    handler.setFormatter(fmt)

    run_filter = RunIdFilter(run_id)
    logger.addFilter(run_filter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ---------------------------------------------------------------------------
# Run metadata tracker
# ---------------------------------------------------------------------------

_DDL_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           VARCHAR NOT NULL,
    start_time       TIMESTAMP NOT NULL,
    end_time         TIMESTAMP,
    status           VARCHAR NOT NULL DEFAULT 'RUNNING',
    input_count      BIGINT,
    output_count     BIGINT,
    rejected_count   BIGINT,
    failure_reason   VARCHAR,
    source_file      VARCHAR,
    PRIMARY KEY (run_id)
)
"""


class RunTracker:
    """
    Records pipeline run lifecycle in DuckDB table `pipeline_runs`.

    Usage
    -----
    tracker = RunTracker(db_path, run_id)
    tracker.start(source_file="data/sample_batch.csv", input_count=1000)
    ...
    tracker.complete(output_count=980, rejected_count=20)
    # or
    tracker.fail(reason="Baseline not found")
    """

    def __init__(self, db_path: str, run_id: str):
        self.db_path = db_path
        self.run_id = run_id
        self._start_ts: Optional[datetime] = None
        self._ensure_table()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _ensure_table(self) -> None:
        with self._connect() as con:
            con.execute(_DDL_PIPELINE_RUNS)

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------

    def start(
        self,
        source_file: str = "",
        input_count: Optional[int] = None,
    ) -> None:
        """Register the start of a run."""
        self._start_ts = datetime.now(timezone.utc)
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO pipeline_runs
                    (run_id, start_time, status, input_count, source_file)
                VALUES (?, ?, 'RUNNING', ?, ?)
                ON CONFLICT (run_id) DO UPDATE SET
                    start_time   = excluded.start_time,
                    status       = 'RUNNING',
                    input_count  = excluded.input_count,
                    source_file  = excluded.source_file
                """,
                [self.run_id, self._start_ts, input_count, source_file],
            )

    def complete(
        self,
        output_count: Optional[int] = None,
        rejected_count: Optional[int] = None,
    ) -> None:
        """Mark run as COMPLETED."""
        end_ts = datetime.now(timezone.utc)
        with self._connect() as con:
            con.execute(
                """
                UPDATE pipeline_runs
                SET end_time       = ?,
                    status         = 'COMPLETED',
                    output_count   = ?,
                    rejected_count = ?
                WHERE run_id = ?
                """,
                [end_ts, output_count, rejected_count, self.run_id],
            )

    def fail(self, reason: str) -> None:
        """Mark run as FAILED with reason."""
        end_ts = datetime.now(timezone.utc)
        with self._connect() as con:
            con.execute(
                """
                UPDATE pipeline_runs
                SET end_time       = ?,
                    status         = 'FAILED',
                    failure_reason = ?
                WHERE run_id = ?
                """,
                [end_ts, reason, self.run_id],
            )

    def get_run_info(self) -> Optional[dict]:
        """Return run metadata dict or None if run not found."""
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM pipeline_runs WHERE run_id = ?", [self.run_id]
            ).fetchdf()
        if rows.empty:
            return None
        return rows.iloc[0].to_dict()

    def elapsed_seconds(self) -> float:
        """Seconds since start() was called."""
        if self._start_ts is None:
            return 0.0
        return (datetime.now(timezone.utc) - self._start_ts).total_seconds()
