"""The Python side of the Rust germline SNV engine: engine switch, writer options, partition of the files."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from radiant.tasks.vcf.snv.germline import occurrence
from radiant.tasks.vcf.snv.rust_engine import ENGINE_ENV, partition_key, sink_config, snv_engine


class Table:
    def __init__(self, properties=None):
        self.metadata = SimpleNamespace(properties=properties or {})

    def schema(self):
        return occurrence.SCHEMA

    def spec(self):
        part = occurrence.SCHEMA.find_field("part")
        task_id = occurrence.SCHEMA.find_field("task_id")
        return PartitionSpec(
            PartitionField(source_id=part.field_id, field_id=1001, transform=IdentityTransform(), name="part"),
            PartitionField(source_id=task_id.field_id, field_id=1002, transform=IdentityTransform(), name="task_id"),
        )

    def name(self):
        return "mock.germline_snv_occurrence"


@pytest.mark.parametrize(
    ("raw", "expected"), [(None, "python"), ("", "python"), ("RUST", "rust"), (" python ", "python")]
)
def test_engine_switch(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv(ENGINE_ENV, raising=False)
    else:
        monkeypatch.setenv(ENGINE_ENV, raw)
    assert snv_engine() == expected


def test_unknown_engine_raises(monkeypatch):
    monkeypatch.setenv(ENGINE_ENV, "go")
    with pytest.raises(ValueError, match=ENGINE_ENV):
        snv_engine()


def test_sink_config_defaults(monkeypatch):
    """PyIceberg's defaults: zstd, 1 MiB pages, 2 MiB dictionaries, 20 000-row batches, 1 Mi-row groups, 512 MiB."""
    monkeypatch.setenv("RADIANT_PARQUET_FILE_SIZE_MB", "64")
    cfg = sink_config(Table(), "/tmp/dir", "occurrence")
    assert cfg["schema"].field("task_id").metadata is None
    assert {k: v for k, v in cfg.items() if k != "schema"} == {
        "directory": "/tmp/dir",
        "prefix": "occurrence",
        "compression": "zstd",
        "compression_level": None,
        "data_page_size": 1024 * 1024,
        "dictionary_page_size": 2 * 1024 * 1024,
        "write_batch_size": 20000,
        "row_group_rows": 1048576,
        "row_group_bytes": 64 * 1024 * 1024,
        "target_file_size": 512 * 1024 * 1024,
    }


def test_sink_config_follows_table_properties():
    props = {
        "write.parquet.compression-codec": "snappy",
        "write.parquet.compression-level": "3",
        "write.parquet.row-group-limit": "1000",
        "write.target-file-size-bytes": "1024",
    }
    cfg = sink_config(Table(props), "/d", "p")
    assert (cfg["compression"], cfg["compression_level"], cfg["row_group_rows"], cfg["target_file_size"]) == (
        "snappy",
        3,
        1000,
        1024,
    )


def test_partition_key_path():
    key = partition_key(Table(), {"part": 3, "task_id": 7})
    assert key.to_path() == "part=3/task_id=7"


def test_unpartitioned_table_has_no_partition_key(monkeypatch):
    table = Table()
    monkeypatch.setattr(table, "spec", lambda: PartitionSpec())
    assert partition_key(table, {"task_id": 7}) is None


def test_partition_key_needs_every_partition_column():
    with pytest.raises(ValueError, match="partition column part"):
        partition_key(Table(), {"task_id": 7})


def test_tables_are_current():
    """src/tables.rs is generated from occurrence.py: regenerate it with gen_tables.py after a table change.

    Needs no extension, so it runs where the parity tests skip.
    """
    gen_tables = Path(__file__).parents[3] / "rust" / "radiant_vcf" / "gen_tables.py"
    spec = importlib.util.spec_from_file_location("gen_tables", gen_tables)
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    assert gen.OUT.read_text() == gen.render(), "rust/radiant_vcf/src/tables.rs is stale: run gen_tables.py"
