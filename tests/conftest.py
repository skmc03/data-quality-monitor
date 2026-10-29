"""
conftest.py — shared pytest fixtures for the data quality monitor test suite.
"""

from __future__ import annotations

import os
import tempfile
import textwrap
from datetime import datetime, timedelta, timezone
from typing import Generator

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEED = 42
RNG = np.random.default_rng(SEED)

_NOW = datetime(2026, 10, 29, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Minimal valid row factory
# ---------------------------------------------------------------------------

def make_valid_row(
    transaction_id: str = "TXN_000001",
    customer_id: str = "CUST_00001",
    amount: float = 150.00,
    currency: str = "USD",
    category: str = "retail",
    merchant_id: str = "MERCH_0001",
    status: str = "completed",
    country_code: str = "US",
    is_flagged: object = False,
    days_ago: int = 1,
) -> dict:
    return {
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "transaction_date": _iso(_NOW - timedelta(days=days_ago)),
        "amount": amount,
        "currency": currency,
        "category": category,
        "merchant_id": merchant_id,
        "status": status,
        "country_code": country_code,
        "is_flagged": is_flagged,
    }


# ---------------------------------------------------------------------------
# Sample DataFrames
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_valid_df() -> pd.DataFrame:
    """100-row clean DataFrame that should pass all validations."""
    rng = np.random.default_rng(SEED)
    currencies = ["USD", "EUR", "GBP", "INR"]
    categories = ["retail", "travel", "food", "utilities", "healthcare"]
    statuses = ["completed", "pending", "failed", "refunded"]
    countries = ["US", "GB", "DE", "IN", "FR"]

    rows = []
    for i in range(100):
        rows.append(
            make_valid_row(
                transaction_id=f"TXN_{i:06d}",
                customer_id=f"CUST_{rng.integers(1, 100):04d}",
                amount=round(float(rng.uniform(10, 2000)), 2),
                currency=str(rng.choice(currencies)),
                category=str(rng.choice(categories)),
                status=str(rng.choice(statuses)),
                country_code=str(rng.choice(countries)),
                is_flagged=bool(rng.integers(0, 2)),
                days_ago=int(rng.integers(1, 20)),
            )
        )
    return pd.DataFrame(rows)


@pytest.fixture()
def sample_baseline_df(sample_valid_df: pd.DataFrame) -> pd.DataFrame:
    """Alias for the baseline dataset fixture."""
    return sample_valid_df.copy()


@pytest.fixture()
def sample_batch_df(sample_valid_df: pd.DataFrame) -> pd.DataFrame:
    """
    50-row batch with intentional drift:
    - Higher null rate on is_flagged
    - Higher amounts
    """
    rng = np.random.default_rng(SEED + 10)
    currencies = ["USD", "EUR", "GBP", "INR"]
    categories = ["retail", "travel", "food", "utilities", "healthcare"]
    statuses = ["completed", "pending", "failed", "refunded"]
    countries = ["US", "GB", "DE", "IN", "FR"]

    rows = []
    for i in range(50):
        is_flagged: object = None if rng.random() < 0.30 else bool(rng.integers(0, 2))
        rows.append(
            make_valid_row(
                transaction_id=f"TXN_BATCH_{i:06d}",
                customer_id=f"CUST_{rng.integers(1, 100):04d}",
                amount=round(float(rng.uniform(5000, 50000)), 2),
                currency=str(rng.choice(currencies)),
                category=str(rng.choice(categories)),
                status=str(rng.choice(statuses)),
                country_code=str(rng.choice(countries)),
                is_flagged=is_flagged,
                days_ago=int(rng.integers(1, 5)),
            )
        )
    return pd.DataFrame(rows)


@pytest.fixture()
def df_with_metadata(sample_valid_df: pd.DataFrame) -> pd.DataFrame:
    """sample_valid_df with run_id and source_file columns attached."""
    df = sample_valid_df.copy()
    df["run_id"] = "TEST_RUN_001"
    df["source_file"] = "test_baseline.csv"
    return df


# ---------------------------------------------------------------------------
# Temporary DuckDB
# ---------------------------------------------------------------------------

@pytest.fixture()
def temp_db() -> Generator[str, None, None]:
    """Provide a path to a temporary DuckDB file; delete after test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_pipeline.duckdb")
        yield db_path


# ---------------------------------------------------------------------------
# Temporary CSV files
# ---------------------------------------------------------------------------

@pytest.fixture()
def baseline_csv(tmp_path: object, sample_valid_df: pd.DataFrame) -> str:
    """Write sample_valid_df to a temp CSV and return the path."""
    path = str(tmp_path / "baseline.csv")
    sample_valid_df.to_csv(path, index=False)
    return path


@pytest.fixture()
def batch_csv(tmp_path: object, sample_batch_df: pd.DataFrame) -> str:
    """Write sample_batch_df to a temp CSV and return the path."""
    path = str(tmp_path / "batch.csv")
    sample_batch_df.to_csv(path, index=False)
    return path


@pytest.fixture()
def empty_csv(tmp_path: object) -> str:
    """An empty (zero-byte) CSV file."""
    path = str(tmp_path / "empty.csv")
    open(path, "w").close()
    return path


@pytest.fixture()
def header_only_csv(tmp_path: object) -> str:
    """A CSV file with only a header row and no data."""
    path = str(tmp_path / "header_only.csv")
    pd.DataFrame(columns=[
        "transaction_id", "customer_id", "transaction_date", "amount",
        "currency", "category", "merchant_id", "status", "country_code", "is_flagged"
    ]).to_csv(path, index=False)
    return path


@pytest.fixture()
def malformed_csv(tmp_path: object) -> str:
    """A CSV file with malformed content (inconsistent columns)."""
    path = str(tmp_path / "malformed.csv")
    content = textwrap.dedent("""\
        transaction_id,customer_id,amount
        TXN_001,CUST_001,100
        TXN_002,only_two_cols
        TXN_003,CUST_003,200,extra_col,another_extra
    """)
    with open(path, "w") as fh:
        fh.write(content)
    return path


# ---------------------------------------------------------------------------
# Schema path fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def schema_path() -> str:
    """Path to the real schema.json in the data directory."""
    root = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(root, "data", "schema.json")


# ---------------------------------------------------------------------------
# Config fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def config(temp_db: str, tmp_path: object) -> dict:
    """Minimal config dict pointing to temp paths."""
    return {
        "database": {"path": temp_db},
        "data": {
            "baseline_path": str(tmp_path / "baseline.csv"),
            "batch_path": str(tmp_path / "batch.csv"),
            "schema_path": os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "schema.json"
            ),
        },
        "thresholds": {
            "null_rate_delta": 0.05,
            "volume_delta_pct": 0.20,
            "distribution_psi_threshold": 0.2,
            "freshness_max_lag_hours": 48,
        },
        "output": {
            "alerts_path": str(tmp_path / "alerts.json"),
            "reconciliation_path": str(tmp_path / "reconciliation_report.json"),
            "quarantine_path": str(tmp_path / "quarantine.csv"),
        },
        "logging": {"level": "WARNING"},
    }
