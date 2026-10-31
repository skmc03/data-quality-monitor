"""
tests/test_ingest.py — Tests for pipeline.ingest module.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from pipeline.ingest import load_csv


RUN_ID = "TEST_INGEST_001"


class TestLoadValidCsv:
    def test_load_valid_csv(self, baseline_csv: str) -> None:
        """load_csv on a valid CSV returns a non-empty DataFrame."""
        df = load_csv(baseline_csv, run_id=RUN_ID)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_returns_expected_column_count(self, baseline_csv: str) -> None:
        """Returned DataFrame has source columns plus run_id + source_file."""
        df = load_csv(baseline_csv, run_id=RUN_ID)
        assert "run_id" in df.columns
        assert "source_file" in df.columns

    def test_all_source_columns_present(self, baseline_csv: str, sample_valid_df: pd.DataFrame) -> None:
        """All columns from the source CSV survive ingestion."""
        df = load_csv(baseline_csv, run_id=RUN_ID)
        for col in sample_valid_df.columns:
            assert col in df.columns, f"Missing column: {col}"


class TestRunIdAttached:
    def test_run_id_attached_to_all_rows(self, baseline_csv: str) -> None:
        """Every row must have the correct run_id value."""
        df = load_csv(baseline_csv, run_id=RUN_ID)
        assert (df["run_id"] == RUN_ID).all(), "Some rows are missing run_id"

    def test_run_id_is_string(self, baseline_csv: str) -> None:
        df = load_csv(baseline_csv, run_id=RUN_ID)
        assert df["run_id"].dtype == object or pd.api.types.is_string_dtype(df["run_id"])


class TestSourceFileAttached:
    def test_source_file_attached_to_all_rows(self, baseline_csv: str) -> None:
        """Every row must have the source_file column populated."""
        df = load_csv(baseline_csv, run_id=RUN_ID)
        assert df["source_file"].notna().all(), "Some rows are missing source_file"

    def test_source_file_is_basename(self, baseline_csv: str) -> None:
        """source_file should be the basename of the file path, not the full path."""
        df = load_csv(baseline_csv, run_id=RUN_ID)
        expected_basename = os.path.basename(baseline_csv)
        assert (df["source_file"] == expected_basename).all()


class TestEmptyFile:
    def test_empty_file_returns_empty_df(self, empty_csv: str) -> None:
        """A zero-byte file returns an empty DataFrame without raising."""
        df = load_csv(empty_csv, run_id=RUN_ID)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_header_only_returns_empty_df(self, header_only_csv: str) -> None:
        """A CSV with only a header row returns an empty DataFrame."""
        df = load_csv(header_only_csv, run_id=RUN_ID)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestMissingFile:
    def test_missing_file_raises_error(self, tmp_path) -> None:
        """A non-existent file path raises FileNotFoundError."""
        missing_path = str(tmp_path / "does_not_exist.csv")
        with pytest.raises(FileNotFoundError):
            load_csv(missing_path, run_id=RUN_ID)

    def test_missing_file_error_mentions_path(self, tmp_path) -> None:
        """The FileNotFoundError message includes the bad path."""
        missing_path = str(tmp_path / "ghost_file.csv")
        with pytest.raises(FileNotFoundError, match="ghost_file.csv"):
            load_csv(missing_path, run_id=RUN_ID)


class TestRowCount:
    def test_row_count_matches_source(
        self, baseline_csv: str, sample_valid_df: pd.DataFrame
    ) -> None:
        """Ingested row count equals source DataFrame row count."""
        df = load_csv(baseline_csv, run_id=RUN_ID)
        assert len(df) == len(sample_valid_df)
