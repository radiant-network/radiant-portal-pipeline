from unittest.mock import MagicMock, patch

import pytest

from radiant.tasks.iceberg.table_accumulator import (
    PARQUET_FILE_SIZE_MB,
    TableAccumulator,
    resolve_parquet_file_size_mb,
)


@pytest.fixture
def iceberg_table():
    """A table whose Arrow schema conversion is stubbed; only the threshold matters here."""
    table = MagicMock()
    with patch.object(TableAccumulator, "remove_field_ids", return_value=MagicMock()):
        yield table


def test_default_is_the_module_constant(monkeypatch):
    monkeypatch.delenv("RADIANT_PARQUET_FILE_SIZE_MB", raising=False)
    assert resolve_parquet_file_size_mb() == PARQUET_FILE_SIZE_MB == 500


def test_env_var_overrides_the_default(monkeypatch):
    monkeypatch.setenv("RADIANT_PARQUET_FILE_SIZE_MB", "64")
    assert resolve_parquet_file_size_mb() == 64


def test_empty_env_var_falls_back_to_the_default(monkeypatch):
    """KubernetesPodOperator turns an env var it was given as None into an empty string.

    Without this, an unset override would reach the container as "" and raise ValueError.
    """
    monkeypatch.setenv("RADIANT_PARQUET_FILE_SIZE_MB", "")
    assert resolve_parquet_file_size_mb() == PARQUET_FILE_SIZE_MB


def test_accumulator_picks_up_the_env_var(monkeypatch, iceberg_table):
    monkeypatch.setenv("RADIANT_PARQUET_FILE_SIZE_MB", "64")

    # Resolved per instance, not bound once as a default argument at import time.
    assert TableAccumulator(iceberg_table).parquet_file_size_mb == 64


def test_explicit_argument_wins_over_the_env_var(monkeypatch, iceberg_table):
    monkeypatch.setenv("RADIANT_PARQUET_FILE_SIZE_MB", "64")

    assert TableAccumulator(iceberg_table, parquet_file_size_mb=128).parquet_file_size_mb == 128


def test_accumulator_defaults_without_the_env_var(monkeypatch, iceberg_table):
    monkeypatch.delenv("RADIANT_PARQUET_FILE_SIZE_MB", raising=False)

    assert TableAccumulator(iceberg_table).parquet_file_size_mb == PARQUET_FILE_SIZE_MB
