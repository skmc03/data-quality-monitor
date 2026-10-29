"""
Ingest module — load CSV source data and attach pipeline metadata.

Responsibilities
----------------
1. Read a CSV file into a Pandas DataFrame.
2. Attach `run_id` and `source_file` columns to every row.
3. Handle edge-cases: missing file, empty file, encoding errors.

The returned DataFrame is the raw ingested frame — no validation yet.
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from pipeline.logger import get_logger


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_csv(
    file_path: str,
    run_id: str,
    *,
    encoding: str = "utf-8",
    parse_dates: bool = False,
    log_level: str = "INFO",
) -> pd.DataFrame:
    """
    Load a CSV file and stamp every row with ``run_id`` and ``source_file``.

    Parameters
    ----------
    file_path:   Absolute or relative path to the CSV file.
    run_id:      Pipeline run identifier (attached as a column).
    encoding:    File encoding (default utf-8).
    parse_dates: If True, attempt to parse date-like columns automatically.
    log_level:   Logging level.

    Returns
    -------
    pd.DataFrame with at least the source columns plus:
        - ``run_id``     : str  — the provided run_id
        - ``source_file``: str  — basename of the file

    Raises
    ------
    FileNotFoundError  if the file does not exist.
    ValueError         if the file cannot be parsed as CSV.
    """
    logger = get_logger(__name__, run_id=run_id, level=log_level)

    # ---- Existence check --------------------------------------------------
    if not os.path.exists(file_path):
        msg = f"Source file not found: {file_path}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    # ---- Empty file -------------------------------------------------------
    if os.path.getsize(file_path) == 0:
        logger.warning("Source file is empty: %s — returning empty DataFrame", file_path)
        return _empty_df_with_metadata()

    # ---- Read CSV ---------------------------------------------------------
    try:
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            low_memory=False,
        )
    except UnicodeDecodeError:
        logger.warning(
            "UTF-8 decode failed for %s; retrying with latin-1", file_path
        )
        try:
            df = pd.read_csv(file_path, encoding="latin-1", low_memory=False)
        except Exception as exc:
            msg = f"Cannot read CSV {file_path}: {exc}"
            logger.error(msg)
            raise ValueError(msg) from exc
    except pd.errors.EmptyDataError:
        logger.warning("EmptyDataError for %s — returning empty DataFrame", file_path)
        return _empty_df_with_metadata()
    except Exception as exc:
        msg = f"Cannot parse CSV {file_path}: {exc}"
        logger.error(msg)
        raise ValueError(msg) from exc

    # ---- Empty after parsing (header only) --------------------------------
    if df.empty:
        logger.warning("CSV has header but no data rows: %s", file_path)
        df = _ensure_metadata_columns(df, run_id, file_path)
        return df

    # ---- Attach metadata --------------------------------------------------
    df = _ensure_metadata_columns(df, run_id, file_path)

    logger.info(
        "Ingested %d rows from %s", len(df), os.path.basename(file_path)
    )
    return df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_metadata_columns(
    df: pd.DataFrame, run_id: str, file_path: str
) -> pd.DataFrame:
    """Attach run_id and source_file to every row."""
    df = df.copy()
    df["run_id"] = run_id
    df["source_file"] = os.path.basename(file_path)
    return df


def _empty_df_with_metadata() -> pd.DataFrame:
    """Return an empty DataFrame with metadata columns present."""
    return pd.DataFrame(columns=["run_id", "source_file"])
