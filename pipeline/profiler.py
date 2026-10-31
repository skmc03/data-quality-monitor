from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd

from pipeline.logger import get_logger


_DDL_COLUMN_PROFILES = """
CREATE TABLE IF NOT EXISTS column_profiles (
    run_id          VARCHAR NOT NULL,
    column_name     VARCHAR NOT NULL,
    profile_type    VARCHAR NOT NULL,
    null_rate       DOUBLE,
    row_count       BIGINT,
    stat_min        DOUBLE,
    stat_max        DOUBLE,
    stat_mean       DOUBLE,
    stat_median     DOUBLE,
    stat_std        DOUBLE,
    stat_p5         DOUBLE,
    stat_p25        DOUBLE,
    stat_p75        DOUBLE,
    stat_p95        DOUBLE,
    value_counts_json VARCHAR,
    freshness_min   TIMESTAMP,
    freshness_max   TIMESTAMP,
    profiled_at     TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, column_name)
)
"""

_TOP_N_CATEGORIES = 20

_DEFAULT_NUMERIC_COLUMNS = ["amount"]
_DEFAULT_CATEGORICAL_COLUMNS = ["currency", "category", "merchant_id", "status", "country_code", "is_flagged"]
_TIMESTAMP_COLUMNS = ["transaction_date"]


def _derive_columns_from_schema(schema_path: Optional[str]) -> tuple[list[str], list[str]]:
    if not schema_path or not os.path.exists(schema_path):
        return _DEFAULT_NUMERIC_COLUMNS, _DEFAULT_CATEGORICAL_COLUMNS

    with open(schema_path, "r", encoding="utf-8") as fh:
        schema = json.load(fh)

    numeric_cols, categorical_cols = [], []
    for field in schema.get("fields", []):
        name = field.get("name", "")
        ftype = field.get("type", "")
        if name in ("transaction_id", "transaction_date"):
            continue
        if ftype in ("float", "integer", "number"):
            numeric_cols.append(name)
        elif ftype in ("string", "boolean"):
            categorical_cols.append(name)

    return (
        numeric_cols or _DEFAULT_NUMERIC_COLUMNS,
        categorical_cols or _DEFAULT_CATEGORICAL_COLUMNS,
    )


def profile(
    df: pd.DataFrame,
    run_id: str,
    db_path: str,
    *,
    schema_path: Optional[str] = None,
    log_level: str = "INFO",
    force: bool = False,
) -> Dict[str, Any]:
    logger = get_logger(__name__, run_id=run_id, level=log_level)

    _ensure_table(db_path)

    if not force and _profile_exists(db_path, run_id):
        logger.info("Profile already exists for run_id=%s — loading from DB", run_id)
        return load_profile(db_path, run_id)

    numeric_cols, categorical_cols = _derive_columns_from_schema(schema_path)

    if df.empty:
        logger.warning("Empty DataFrame; producing minimal profile.")
        profile_data = _minimal_profile(run_id)
    else:
        profile_data = _compute_profile(df, run_id, logger, numeric_cols, categorical_cols)

    _persist_profile(profile_data, db_path, run_id, logger)

    logger.info(
        "Profile computed and stored for run_id=%s (%d columns)",
        run_id,
        len(profile_data),
    )
    return profile_data


def load_profile(db_path: str, run_id: str) -> Dict[str, Any]:
    with duckdb.connect(db_path) as con:
        rows = con.execute(
            "SELECT * FROM column_profiles WHERE run_id = ?", [run_id]
        ).fetchdf()

    if rows.empty:
        return {}

    result: Dict[str, Any] = {}
    for _, row in rows.iterrows():
        col = row["column_name"]
        profile_row: Dict[str, Any] = {
            "profile_type": row["profile_type"],
            "null_rate": row["null_rate"],
            "row_count": int(row["row_count"]) if pd.notna(row["row_count"]) else 0,
        }

        if row["profile_type"] == "numeric":
            profile_row.update(
                {
                    "min": row["stat_min"],
                    "max": row["stat_max"],
                    "mean": row["stat_mean"],
                    "median": row["stat_median"],
                    "std": row["stat_std"],
                    "p5": row["stat_p5"],
                    "p25": row["stat_p25"],
                    "p75": row["stat_p75"],
                    "p95": row["stat_p95"],
                }
            )
        elif row["profile_type"] == "categorical":
            vc_json = row.get("value_counts_json")
            profile_row["value_counts"] = json.loads(vc_json) if vc_json else {}
        elif row["profile_type"] == "freshness":
            profile_row["freshness_min"] = str(row["freshness_min"]) if pd.notna(row["freshness_min"]) else None
            profile_row["freshness_max"] = str(row["freshness_max"]) if pd.notna(row["freshness_max"]) else None

        result[col] = profile_row

    return result


def get_latest_baseline_run_id(db_path: str, exclude_run_id: Optional[str] = None) -> Optional[str]:
    with duckdb.connect(db_path) as con:
        try:
            if exclude_run_id:
                row = con.execute(
                    """
                    SELECT run_id FROM column_profiles
                    WHERE run_id != ?
                    ORDER BY profiled_at DESC
                    LIMIT 1
                    """,
                    [exclude_run_id],
                ).fetchone()
            else:
                row = con.execute(
                    """
                    SELECT run_id FROM column_profiles
                    ORDER BY profiled_at DESC
                    LIMIT 1
                    """
                ).fetchone()
        except Exception:
            return None
    return row[0] if row else None


def _compute_profile(
    df: pd.DataFrame,
    run_id: str,
    logger,
    numeric_cols: list[str],
    categorical_cols: list[str],
) -> Dict[str, Any]:
    profile_data: Dict[str, Any] = {}
    total_rows = len(df)

    profile_data["_volume"] = {
        "profile_type": "volume",
        "null_rate": 0.0,
        "row_count": total_rows,
    }

    for col in numeric_cols:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        null_rate = float(series.isna().sum()) / total_rows if total_rows > 0 else 0.0
        non_null = series.dropna()

        if non_null.empty:
            profile_data[col] = {
                "profile_type": "numeric",
                "null_rate": null_rate,
                "row_count": total_rows,
                "min": None, "max": None, "mean": None,
                "median": None, "std": None,
                "p5": None, "p25": None, "p75": None, "p95": None,
            }
        else:
            profile_data[col] = {
                "profile_type": "numeric",
                "null_rate": null_rate,
                "row_count": total_rows,
                "min": float(non_null.min()),
                "max": float(non_null.max()),
                "mean": float(non_null.mean()),
                "median": float(non_null.median()),
                "std": float(non_null.std()),
                "p5": float(np.percentile(non_null, 5)),
                "p25": float(np.percentile(non_null, 25)),
                "p75": float(np.percentile(non_null, 75)),
                "p95": float(np.percentile(non_null, 95)),
            }

    for col in categorical_cols:
        if col not in df.columns:
            continue
        null_rate = float(df[col].isna().sum()) / total_rows if total_rows > 0 else 0.0
        non_null = df[col].dropna()

        if non_null.empty:
            vc = {}
        else:
            counts = non_null.astype(str).value_counts(normalize=True)
            vc = {k: round(float(v), 6) for k, v in counts.head(_TOP_N_CATEGORIES).items()}

        profile_data[col] = {
            "profile_type": "categorical",
            "null_rate": null_rate,
            "row_count": total_rows,
            "value_counts": vc,
        }

    if "transaction_date" in df.columns:
        ts_series = pd.to_datetime(df["transaction_date"], errors="coerce", utc=True)
        null_rate = float(ts_series.isna().sum()) / total_rows if total_rows > 0 else 0.0
        non_null_ts = ts_series.dropna()
        if non_null_ts.empty:
            freshness_min = freshness_max = None
        else:
            freshness_min = non_null_ts.min().isoformat()
            freshness_max = non_null_ts.max().isoformat()

        profile_data["transaction_date"] = {
            "profile_type": "freshness",
            "null_rate": null_rate,
            "row_count": total_rows,
            "freshness_min": freshness_min,
            "freshness_max": freshness_max,
        }

    return profile_data


def _minimal_profile(run_id: str) -> Dict[str, Any]:
    return {
        "_volume": {
            "profile_type": "volume",
            "null_rate": 0.0,
            "row_count": 0,
        }
    }


def _ensure_table(db_path: str) -> None:
    with duckdb.connect(db_path) as con:
        con.execute(_DDL_COLUMN_PROFILES)


def _profile_exists(db_path: str, run_id: str) -> bool:
    with duckdb.connect(db_path) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM column_profiles WHERE run_id = ?", [run_id]
        ).fetchone()[0]
    return count > 0


def _persist_profile(profile_data: Dict[str, Any], db_path: str, run_id: str, logger) -> None:
    ts_now = datetime.now(timezone.utc)

    rows: List[dict] = []
    for col_name, pdata in profile_data.items():
        row: Dict[str, Any] = {
            "run_id": run_id,
            "column_name": col_name,
            "profile_type": pdata.get("profile_type", "unknown"),
            "null_rate": pdata.get("null_rate"),
            "row_count": pdata.get("row_count"),
            "stat_min": pdata.get("min"),
            "stat_max": pdata.get("max"),
            "stat_mean": pdata.get("mean"),
            "stat_median": pdata.get("median"),
            "stat_std": pdata.get("std"),
            "stat_p5": pdata.get("p5"),
            "stat_p25": pdata.get("p25"),
            "stat_p75": pdata.get("p75"),
            "stat_p95": pdata.get("p95"),
            "value_counts_json": json.dumps(pdata.get("value_counts", {})) if pdata.get("value_counts") is not None else None,
            "freshness_min": pdata.get("freshness_min"),
            "freshness_max": pdata.get("freshness_max"),
            "profiled_at": ts_now,
        }
        rows.append(row)

    if not rows:
        return

    insert_df = pd.DataFrame(rows)

    for col in ("freshness_min", "freshness_max"):
        insert_df[col] = pd.to_datetime(insert_df[col], errors="coerce", utc=True)

    with duckdb.connect(db_path) as con:
        con.execute(_DDL_COLUMN_PROFILES)
        con.execute("DELETE FROM column_profiles WHERE run_id = ?", [run_id])
        con.register("_profile_insert", insert_df)
        con.execute(
            """
            INSERT INTO column_profiles
            SELECT
                run_id, column_name, profile_type, null_rate, row_count,
                stat_min, stat_max, stat_mean, stat_median, stat_std,
                stat_p5, stat_p25, stat_p75, stat_p95,
                value_counts_json,
                freshness_min, freshness_max, profiled_at
            FROM _profile_insert
            """
        )
