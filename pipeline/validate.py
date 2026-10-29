"""
Validate module — schema validation, business rules, duplicate detection.

Rules applied (in order)
------------------------
1.  Required columns present (schema check).
2.  Required fields not null.
3.  Type coercion check (amount must be numeric).
4.  Business rule: amount > 0.
5.  Business rule: currency in allowed enum.
6.  Business rule: category in allowed enum.
7.  Business rule: status in allowed enum.
8.  Business rule: country_code is exactly 2 characters.
9.  Referential integrity: customer_id must exist in customers.csv.
10. Referential integrity: merchant_id must exist in merchants.csv.
11. Duplicate transaction_id detection.

Returns a (valid_df, rejected_df) tuple.
rejected_df carries original columns plus:
    rejection_reason, run_id, source_file
"""

from __future__ import annotations

import json
import os
from typing import Optional, Set, Tuple

import pandas as pd

from pipeline.logger import get_logger

# Reference data paths (resolved relative to project root)
_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
_CUSTOMERS_CSV = os.path.join(_DATA_DIR, "customers.csv")
_MERCHANTS_CSV = os.path.join(_DATA_DIR, "merchants.csv")


def _load_reference_set(path: str) -> Optional[Set[str]]:
    """Load a single-column reference CSV into a set of valid IDs. Returns None if file missing."""
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, dtype=str)
    return set(df.iloc[:, 0].dropna().str.strip())


# ---------------------------------------------------------------------------
# Default allowed values (overridable via schema.json)
# ---------------------------------------------------------------------------

_DEFAULT_ALLOWED_CURRENCIES = {"USD", "EUR", "GBP", "INR"}
_DEFAULT_ALLOWED_CATEGORIES = {"retail", "travel", "food", "utilities", "healthcare"}
_DEFAULT_ALLOWED_STATUSES = {"completed", "pending", "failed", "refunded"}
_REQUIRED_COLUMNS = [
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate(
    df: pd.DataFrame,
    run_id: str,
    *,
    schema_path: Optional[str] = None,
    log_level: str = "INFO",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate a DataFrame against schema and business rules.

    Parameters
    ----------
    df:          Raw ingested DataFrame (must already have run_id, source_file).
    run_id:      Pipeline run identifier.
    schema_path: Path to schema.json (optional; uses defaults if None).
    log_level:   Logging level.

    Returns
    -------
    (valid_df, rejected_df)
    valid_df    — rows that passed all checks
    rejected_df — rows that failed, with `rejection_reason` column
    """
    logger = get_logger(__name__, run_id=run_id, level=log_level)
    schema = _load_schema(schema_path) if schema_path else {}
    allowed = _build_allowed(schema)

    if df.empty:
        logger.warning("Empty DataFrame passed to validate(); returning empty splits.")
        return df.copy(), _empty_rejected()

    # Track rejection reasons: index -> list[str]
    rejection_reasons: dict[int, list[str]] = {i: [] for i in df.index}

    # 1. Required column presence (whole-file check)
    missing_cols = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        logger.error("Missing required columns: %s", missing_cols)
        # All rows are rejected if required columns are absent
        all_rejected = df.copy()
        all_rejected["rejection_reason"] = f"Missing columns: {missing_cols}"
        return _empty_valid(df), all_rejected

    # 2. Required-not-null checks
    required_not_null = [
        "transaction_id",
        "customer_id",
        "transaction_date",
        "amount",
        "currency",
        "category",
        "merchant_id",
        "status",
        "country_code",
    ]
    for col in required_not_null:
        null_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        for idx in df.index[null_mask]:
            rejection_reasons[idx].append(f"NULL required field: {col}")

    # 3. Amount must be numeric and > 0
    try:
        amounts = pd.to_numeric(df["amount"], errors="coerce")
    except Exception:
        amounts = pd.Series([None] * len(df), index=df.index)

    non_numeric_mask = amounts.isna() & df["amount"].notna()
    for idx in df.index[non_numeric_mask]:
        rejection_reasons[idx].append("amount is not numeric")

    zero_or_neg_mask = amounts.notna() & (amounts <= 0)
    for idx in df.index[zero_or_neg_mask]:
        rejection_reasons[idx].append(f"amount <= 0 (got {df.at[idx, 'amount']})")

    # 4. Currency enum check
    bad_currency = df["currency"].notna() & ~df["currency"].isin(allowed["currencies"])
    for idx in df.index[bad_currency]:
        rejection_reasons[idx].append(
            f"invalid currency '{df.at[idx, 'currency']}'; allowed={sorted(allowed['currencies'])}"
        )

    # 5. Category enum check
    bad_category = df["category"].notna() & ~df["category"].isin(allowed["categories"])
    for idx in df.index[bad_category]:
        rejection_reasons[idx].append(
            f"invalid category '{df.at[idx, 'category']}'; allowed={sorted(allowed['categories'])}"
        )

    # 6. Status enum check
    bad_status = df["status"].notna() & ~df["status"].isin(allowed["statuses"])
    for idx in df.index[bad_status]:
        rejection_reasons[idx].append(
            f"invalid status '{df.at[idx, 'status']}'; allowed={sorted(allowed['statuses'])}"
        )

    # 7. country_code must be exactly 2 characters
    bad_country = df["country_code"].notna() & (
        df["country_code"].astype(str).str.len() != 2
    )
    for idx in df.index[bad_country]:
        rejection_reasons[idx].append(
            f"country_code must be 2 chars (got '{df.at[idx, 'country_code']}')"
        )

    # 8. Referential integrity: customer_id must exist in customers.csv
    known_customers = _load_reference_set(_CUSTOMERS_CSV)
    if known_customers is not None:
        bad_customer_ref = (
            df["customer_id"].notna() &
            ~df["customer_id"].astype(str).isin(known_customers)
        )
        for idx in df.index[bad_customer_ref]:
            rejection_reasons[idx].append(
                f"unknown_customer_id '{df.at[idx, 'customer_id']}'"
            )

    # 9. Referential integrity: merchant_id must exist in merchants.csv
    known_merchants = _load_reference_set(_MERCHANTS_CSV)
    if known_merchants is not None:
        bad_merchant_ref = (
            df["merchant_id"].notna() &
            ~df["merchant_id"].astype(str).isin(known_merchants)
        )
        for idx in df.index[bad_merchant_ref]:
            rejection_reasons[idx].append(
                f"unknown_merchant_id '{df.at[idx, 'merchant_id']}'"
            )

    # 11. Duplicate transaction_id detection — keep first occurrence
    dup_mask = df.duplicated(subset=["transaction_id"], keep="first")
    for idx in df.index[dup_mask]:
        rejection_reasons[idx].append(
            f"duplicate transaction_id '{df.at[idx, 'transaction_id']}'"
        )

    # --- Split valid / rejected ---
    rejected_indices = [i for i, reasons in rejection_reasons.items() if reasons]
    valid_indices = [i for i in df.index if i not in set(rejected_indices)]

    valid_df = df.loc[valid_indices].copy().reset_index(drop=True)

    if rejected_indices:
        rejected_df = df.loc[rejected_indices].copy()
        rejected_df["rejection_reason"] = [
            "; ".join(rejection_reasons[i]) for i in rejected_indices
        ]
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        rejected_df = _empty_rejected()

    logger.info(
        "Validation complete: %d valid, %d rejected (from %d input rows)",
        len(valid_df),
        len(rejected_df),
        len(df),
    )
    return valid_df, rejected_df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_schema(schema_path: str) -> dict:
    if not os.path.exists(schema_path):
        return {}
    with open(schema_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_allowed(schema: dict) -> dict:
    """Extract allowed value sets from schema.json or fall back to defaults."""
    currencies = set(_DEFAULT_ALLOWED_CURRENCIES)
    categories = set(_DEFAULT_ALLOWED_CATEGORIES)
    statuses = set(_DEFAULT_ALLOWED_STATUSES)

    for field in schema.get("fields", []):
        name = field.get("name", "")
        allowed_vals = field.get("allowed_values")
        if allowed_vals:
            if name == "currency":
                currencies = set(allowed_vals)
            elif name == "category":
                categories = set(allowed_vals)
            elif name == "status":
                statuses = set(allowed_vals)

    return {"currencies": currencies, "categories": categories, "statuses": statuses}


def _empty_valid(reference: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(columns=reference.columns)


def _empty_rejected() -> pd.DataFrame:
    cols = _REQUIRED_COLUMNS + ["run_id", "source_file", "rejection_reason"]
    return pd.DataFrame(columns=cols)
