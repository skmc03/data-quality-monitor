import argparse
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

SEED = 42
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
CATEGORIES = ["retail", "travel", "food", "utilities", "healthcare"]
STATUSES = ["completed", "pending", "failed", "refunded"]
COUNTRY_CODES = ["US", "GB", "DE", "FR", "IN", "CA", "AU", "JP", "SG", "AE"]
KNOWN_MERCHANTS = [f"MERCH_{i:04d}" for i in range(1, 201)]
KNOWN_CUSTOMERS = [f"CUST_{i:05d}" for i in range(1, 5001)]

# Fixed reference point so the generator produces identical output on any machine on any day
_REFERENCE_TS = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_baseline(n: int = 1000, rng: np.random.Generator = None) -> pd.DataFrame:
    if rng is None:
        rng = np.random.default_rng(SEED)

    transaction_ids = [f"TXN_BASE_{i:06d}" for i in range(n)]
    customer_ids = rng.choice(KNOWN_CUSTOMERS, size=n).tolist()

    seconds_offsets = rng.integers(0, 30 * 24 * 3600, size=n)
    dates = [_iso(_REFERENCE_TS - timedelta(seconds=int(s))) for s in seconds_offsets]

    raw_amounts = rng.lognormal(mean=5.5, sigma=1.0, size=n)
    amounts = np.clip(raw_amounts, 10.0, 5000.0).round(2)

    currencies = rng.choice(CURRENCIES, size=n, p=[0.55, 0.20, 0.15, 0.10])
    categories = rng.choice(CATEGORIES, size=n)
    merchant_ids = rng.choice(KNOWN_MERCHANTS, size=n)
    statuses = rng.choice(STATUSES, size=n, p=[0.70, 0.15, 0.10, 0.05])
    country_codes = rng.choice(COUNTRY_CODES, size=n)

    # ~2% null is_flagged to simulate realistic missing data in baseline
    is_flagged_raw = rng.random(size=n)
    is_flagged: list = []
    for v in is_flagged_raw:
        if v < 0.02:
            is_flagged.append(None)
        elif v < 0.07:
            is_flagged.append(True)
        else:
            is_flagged.append(False)

    return pd.DataFrame(
        {
            "transaction_id": transaction_ids,
            "customer_id": customer_ids,
            "transaction_date": dates,
            "amount": amounts,
            "currency": currencies,
            "category": categories,
            "merchant_id": merchant_ids,
            "status": statuses,
            "country_code": country_codes,
            "is_flagged": is_flagged,
        }
    )


def generate_batch(n: int = 1000, rng: np.random.Generator = None) -> pd.DataFrame:
    if rng is None:
        rng = np.random.default_rng(SEED + 1)

    transaction_ids = [f"TXN_BATCH_{i:06d}" for i in range(n)]
    customer_ids = rng.choice(KNOWN_CUSTOMERS, size=n).tolist()

    seconds_offsets = rng.integers(0, 5 * 24 * 3600, size=n)
    dates = [_iso(_REFERENCE_TS + timedelta(seconds=int(s))) for s in seconds_offsets]

    raw_amounts = rng.lognormal(mean=8.0, sigma=1.5, size=n)
    amounts = np.clip(raw_amounts, 10.0, 500_000.0).round(2)

    currencies = rng.choice(CURRENCIES, size=n, p=[0.55, 0.20, 0.15, 0.10])
    categories = rng.choice(CATEGORIES, size=n)
    merchant_ids = rng.choice(KNOWN_MERCHANTS, size=n)
    statuses = rng.choice(STATUSES, size=n, p=[0.70, 0.15, 0.10, 0.05])
    country_codes = rng.choice(COUNTRY_CODES, size=n)

    # ~15% null is_flagged — intentional drift from baseline's 2%
    is_flagged_raw = rng.random(size=n)
    is_flagged: list = []
    for v in is_flagged_raw:
        if v < 0.15:
            is_flagged.append(None)
        elif v < 0.20:
            is_flagged.append(True)
        else:
            is_flagged.append(False)

    df = pd.DataFrame(
        {
            "transaction_id": transaction_ids,
            "customer_id": customer_ids,
            "transaction_date": dates,
            "amount": amounts,
            "currency": currencies,
            "category": categories,
            "merchant_id": merchant_ids,
            "status": statuses,
            "country_code": country_codes,
            "is_flagged": is_flagged,
        }
    )

    stale_indices = rng.choice(n, size=max(1, n // 10), replace=False)
    for idx in stale_indices:
        stale_offset = rng.integers(58 * 24 * 3600, 62 * 24 * 3600)
        df.at[idx, "transaction_date"] = _iso(_REFERENCE_TS - timedelta(seconds=int(stale_offset)))

    bad_currency_indices = rng.choice(n, size=min(5, n), replace=False)
    for idx in bad_currency_indices:
        df.at[idx, "currency"] = "JPY"

    dup_count = min(20, n // 2)
    source_indices = rng.choice(n // 2, size=dup_count, replace=False)
    dup_target_indices = rng.choice(range(n // 2, n), size=dup_count, replace=False)
    for src, tgt in zip(source_indices, dup_target_indices):
        df.at[tgt, "transaction_id"] = df.at[src, "transaction_id"]

    null_customer_indices = rng.choice(n, size=min(5, n), replace=False)
    for idx in null_customer_indices:
        df.at[idx, "customer_id"] = None

    unknown_merchants = [f"UNKNOWN_MERCH_{i:04d}" for i in range(1, 11)]
    bad_merchant_indices = rng.choice(n, size=min(10, n), replace=False)
    for i, idx in enumerate(bad_merchant_indices):
        df.at[idx, "merchant_id"] = unknown_merchants[i % len(unknown_merchants)]

    late_indices = rng.choice(n, size=min(15, n), replace=False)
    for idx in late_indices:
        late_offset = rng.integers(35 * 24 * 3600, 90 * 24 * 3600)
        df.at[idx, "transaction_date"] = _iso(_REFERENCE_TS - timedelta(seconds=int(late_offset)))

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic financial transaction data")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--baseline-only", action="store_true")
    parser.add_argument("--batch-only", action="store_true")
    parser.add_argument(
        "--out-dir",
        type=str,
        default=os.path.join(os.path.dirname(__file__)),
    )
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    rng_baseline = np.random.default_rng(SEED)
    rng_batch = np.random.default_rng(SEED + 1)

    customers_path = os.path.join(args.out_dir, "customers.csv")
    pd.DataFrame({"customer_id": KNOWN_CUSTOMERS}).to_csv(customers_path, index=False)
    print(f"  -> Reference: {customers_path} ({len(KNOWN_CUSTOMERS)} customers)")

    merchants_path = os.path.join(args.out_dir, "merchants.csv")
    pd.DataFrame({"merchant_id": KNOWN_MERCHANTS}).to_csv(merchants_path, index=False)
    print(f"  -> Reference: {merchants_path} ({len(KNOWN_MERCHANTS)} merchants)")

    if not args.batch_only:
        print(f"Generating baseline ({args.rows} rows)...")
        baseline_df = generate_baseline(n=args.rows, rng=rng_baseline)
        baseline_path = os.path.join(args.out_dir, "sample_baseline.csv")
        baseline_df.to_csv(baseline_path, index=False)
        print(f"  -> Written to {baseline_path}")

    if not args.baseline_only:
        print(f"Generating batch ({args.rows} rows with intentional drift)...")
        batch_df = generate_batch(n=args.rows, rng=rng_batch)
        batch_path = os.path.join(args.out_dir, "sample_batch.csv")
        batch_df.to_csv(batch_path, index=False)
        print(f"  -> Written to {batch_path}")

    print("Done.")


if __name__ == "__main__":
    main()
