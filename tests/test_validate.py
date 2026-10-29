"""
tests/test_validate.py — Tests for pipeline.validate module.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.validate import validate
from tests.conftest import make_valid_row


RUN_ID = "TEST_VALIDATE_001"


def _df(rows: list) -> pd.DataFrame:
    """Helper: build a DataFrame from a list of row dicts, adding metadata cols."""
    df = pd.DataFrame(rows)
    df["run_id"] = RUN_ID
    df["source_file"] = "test_source.csv"
    return df


class TestValidRecordsPass:
    def test_valid_records_pass(self, sample_valid_df: pd.DataFrame) -> None:
        """All rows in sample_valid_df should pass validation."""
        df = sample_valid_df.copy()
        df["run_id"] = RUN_ID
        df["source_file"] = "test.csv"
        valid, rejected = validate(df, run_id=RUN_ID)
        assert len(rejected) == 0, f"Expected 0 rejected, got {len(rejected)}"
        assert len(valid) == len(df)

    def test_valid_single_row(self) -> None:
        """A single well-formed row is accepted."""
        df = _df([make_valid_row()])
        valid, rejected = validate(df, run_id=RUN_ID)
        assert len(valid) == 1
        assert len(rejected) == 0


class TestNullRequiredField:
    def test_null_customer_id_rejected(self) -> None:
        row = make_valid_row()
        row["customer_id"] = None
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1
        assert "customer_id" in rejected.iloc[0]["rejection_reason"]

    def test_null_transaction_id_rejected(self) -> None:
        row = make_valid_row()
        row["transaction_id"] = None
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1

    def test_null_required_field_rejected(self) -> None:
        """Null merchant_id should be rejected."""
        row = make_valid_row()
        row["merchant_id"] = None
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1
        assert "merchant_id" in rejected.iloc[0]["rejection_reason"]

    def test_empty_string_treated_as_null(self) -> None:
        """Empty string in a required field should be rejected."""
        row = make_valid_row()
        row["customer_id"] = ""
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1


class TestInvalidCurrency:
    def test_invalid_currency_rejected(self) -> None:
        row = make_valid_row(currency="JPY")
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1
        assert "currency" in rejected.iloc[0]["rejection_reason"]

    def test_all_valid_currencies_accepted(self) -> None:
        rows = [make_valid_row(transaction_id=f"TXN_{c}", currency=c) for c in ["USD", "EUR", "GBP", "INR"]]
        valid, rejected = validate(_df(rows), run_id=RUN_ID)
        assert len(rejected) == 0
        assert len(valid) == 4


class TestAmountValidation:
    def test_amount_zero_rejected(self) -> None:
        row = make_valid_row(amount=0)
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1
        assert "amount" in rejected.iloc[0]["rejection_reason"]

    def test_amount_negative_rejected(self) -> None:
        row = make_valid_row(amount=-50.0)
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1
        assert "amount" in rejected.iloc[0]["rejection_reason"]

    def test_positive_amount_accepted(self) -> None:
        row = make_valid_row(amount=0.01)
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(valid) == 1

    def test_non_numeric_amount_rejected(self) -> None:
        row = make_valid_row()
        row["amount"] = "not_a_number"
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1


class TestInvalidCountryCode:
    def test_invalid_country_code_rejected(self) -> None:
        row = make_valid_row(country_code="USA")  # 3 chars, invalid
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1
        assert "country_code" in rejected.iloc[0]["rejection_reason"]

    def test_single_char_country_code_rejected(self) -> None:
        row = make_valid_row(country_code="U")
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(rejected) == 1

    def test_two_char_country_code_accepted(self) -> None:
        row = make_valid_row(country_code="US")
        valid, rejected = validate(_df([row]), run_id=RUN_ID)
        assert len(valid) == 1


class TestDuplicateTransactionId:
    def test_duplicate_transaction_id_rejected(self) -> None:
        """Second occurrence of same transaction_id should be rejected."""
        row1 = make_valid_row(transaction_id="TXN_DUP")
        row2 = make_valid_row(transaction_id="TXN_DUP")
        valid, rejected = validate(_df([row1, row2]), run_id=RUN_ID)
        assert len(valid) == 1
        assert len(rejected) == 1
        assert "duplicate" in rejected.iloc[0]["rejection_reason"].lower()

    def test_no_false_duplicate_rejection(self) -> None:
        """Unique transaction_ids must not be flagged as duplicates."""
        rows = [make_valid_row(transaction_id=f"TXN_{i:04d}") for i in range(5)]
        valid, rejected = validate(_df(rows), run_id=RUN_ID)
        assert len(rejected) == 0

    def test_first_occurrence_kept_on_duplicate(self) -> None:
        """First occurrence is in valid_df, second in rejected_df."""
        row1 = make_valid_row(transaction_id="TXN_DUP", amount=100.0)
        row2 = make_valid_row(transaction_id="TXN_DUP", amount=200.0)
        valid, rejected = validate(_df([row1, row2]), run_id=RUN_ID)
        assert valid.iloc[0]["amount"] == 100.0


class TestReturnStructure:
    def test_rejected_df_has_rejection_reason(self) -> None:
        row = make_valid_row(currency="INVALID")
        _, rejected = validate(_df([row]), run_id=RUN_ID)
        assert "rejection_reason" in rejected.columns

    def test_valid_df_has_no_rejection_reason(self) -> None:
        row = make_valid_row()
        valid, _ = validate(_df([row]), run_id=RUN_ID)
        assert "rejection_reason" not in valid.columns

    def test_empty_df_returns_two_empty_dfs(self) -> None:
        df = pd.DataFrame(columns=[
            "transaction_id", "customer_id", "transaction_date", "amount",
            "currency", "category", "merchant_id", "status", "country_code",
            "is_flagged", "run_id", "source_file"
        ])
        valid, rejected = validate(df, run_id=RUN_ID)
        assert len(valid) == 0
        assert len(rejected) == 0
