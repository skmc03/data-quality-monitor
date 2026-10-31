from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import duckdb
import pandas as pd

from pipeline.logger import get_logger


_DDL_QUARANTINE = """
CREATE TABLE IF NOT EXISTS quarantine (
    transaction_id      VARCHAR,
    customer_id         VARCHAR,
    transaction_date    VARCHAR,
    amount              DOUBLE,
    currency            VARCHAR,
    category            VARCHAR,
    merchant_id         VARCHAR,
    status              VARCHAR,
    country_code        VARCHAR,
    is_flagged          BOOLEAN,
    run_id              VARCHAR NOT NULL,
    source_file         VARCHAR,
    rejection_reason    VARCHAR NOT NULL,
    quarantine_timestamp TIMESTAMP NOT NULL
)
"""

_QUARANTINE_COLS = [
    "transaction_id",
    "customer_id",
    "transaction_date",
    "amount",
    "currency",
    "category",
    "merchant_id",
    "status",
    "country_code",
    "is_flagged",
    "run_id",
    "source_file",
    "rejection_reason",
    "quarantine_timestamp",
]


def quarantine_records(
    rejected_df: pd.DataFrame,
    db_path: str,
    run_id: str,
    *,
    quarantine_csv_path: Optional[str] = None,
    log_level: str = "INFO",
) -> int:
    logger = get_logger(__name__, run_id=run_id, level=log_level)

    if rejected_df is None or rejected_df.empty:
        logger.info("No rejected records to quarantine.")
        return 0

    with duckdb.connect(db_path) as con:
        con.execute(_DDL_QUARANTINE)
        existing = con.execute(
            "SELECT COUNT(*) FROM quarantine WHERE run_id = ?", [run_id]
        ).fetchone()[0]
        if existing > 0:
            logger.info(
                "Quarantine already has %d rows for run_id=%s — deleting before re-insert",
                existing,
                run_id,
            )
            con.execute("DELETE FROM quarantine WHERE run_id = ?", [run_id])

    ts_now = datetime.now(timezone.utc)
    insert_df = _build_quarantine_df(rejected_df, run_id, ts_now)

    with duckdb.connect(db_path) as con:
        con.execute(_DDL_QUARANTINE)
        con.register("_quarantine_insert", insert_df)
        con.execute(
            f"INSERT INTO quarantine SELECT {', '.join(_QUARANTINE_COLS)} FROM _quarantine_insert"
        )

    logger.info("Quarantined %d records for run_id=%s", len(insert_df), run_id)

    if quarantine_csv_path:
        _write_quarantine_csv(insert_df, quarantine_csv_path, logger)

    return len(insert_df)


def get_quarantine_records(db_path: str, run_id: Optional[str] = None) -> pd.DataFrame:
    with duckdb.connect(db_path) as con:
        try:
            con.execute(_DDL_QUARANTINE)
            if run_id:
                return con.execute(
                    "SELECT * FROM quarantine WHERE run_id = ?", [run_id]
                ).fetchdf()
            return con.execute("SELECT * FROM quarantine").fetchdf()
        except Exception:
            return pd.DataFrame(columns=_QUARANTINE_COLS)


def _build_quarantine_df(
    rejected_df: pd.DataFrame, run_id: str, ts: datetime
) -> pd.DataFrame:
    df = rejected_df.copy()

    source_cols = [
        "transaction_id",
        "customer_id",
        "transaction_date",
        "amount",
        "currency",
        "category",
        "merchant_id",
        "status",
        "country_code",
        "is_flagged",
    ]
    for col in source_cols:
        if col not in df.columns:
            df[col] = None

    df["run_id"] = run_id
    if "source_file" not in df.columns:
        df["source_file"] = None
    if "rejection_reason" not in df.columns:
        df["rejection_reason"] = "unknown"

    df["quarantine_timestamp"] = ts

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if "is_flagged" in df.columns:
        df["is_flagged"] = df["is_flagged"].map(
            lambda x: None if pd.isna(x) else bool(x)
        )

    return df[_QUARANTINE_COLS].reset_index(drop=True)


def _write_quarantine_csv(
    df: pd.DataFrame, path: str, logger
) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    df.to_csv(path, mode="a", header=write_header, index=False)
    logger.info("Quarantine CSV updated: %s", path)
