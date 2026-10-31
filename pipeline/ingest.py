from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from pipeline.logger import get_logger


def load_csv(
    file_path: str,
    run_id: str,
    *,
    encoding: str = "utf-8",
    parse_dates: bool = False,
    log_level: str = "INFO",
) -> pd.DataFrame:
    logger = get_logger(__name__, run_id=run_id, level=log_level)

    if not os.path.exists(file_path):
        msg = f"Source file not found: {file_path}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    if os.path.getsize(file_path) == 0:
        logger.warning("Source file is empty: %s — returning empty DataFrame", file_path)
        return _empty_df_with_metadata()

    try:
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            low_memory=False,
        )
    except UnicodeDecodeError:
        logger.warning("UTF-8 decode failed for %s; retrying with latin-1", file_path)
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

    if df.empty:
        logger.warning("CSV has header but no data rows: %s", file_path)
        df = _ensure_metadata_columns(df, run_id, file_path)
        return df

    df = _ensure_metadata_columns(df, run_id, file_path)

    logger.info("Ingested %d rows from %s", len(df), os.path.basename(file_path))
    return df


def _ensure_metadata_columns(
    df: pd.DataFrame, run_id: str, file_path: str
) -> pd.DataFrame:
    df = df.copy()
    df["run_id"] = run_id
    df["source_file"] = os.path.basename(file_path)
    return df


def _empty_df_with_metadata() -> pd.DataFrame:
    return pd.DataFrame(columns=["run_id", "source_file"])
