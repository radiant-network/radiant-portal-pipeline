"""The rows P4 writes to `open_data_release` (SJRA-1811 §4).

The table answers "which release did the portal-facing tables get annotated against?". A row is only
worth reading if it names the schema the source was *actually* read from, so these cover the
hold-back paths rather than the happy one.
"""

import pytest

from radiant.tasks.data.open_data import build_open_data_release_rows
from radiant.tasks.data.radiant_tables import (
    ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
    ICEBERG_OPEN_DATA_LEGACY_MAPPING,
)

_CONF = {
    "RADIANT_ICEBERG_CATALOG": "radiant_iceberg_catalog",
    "RADIANT_ICEBERG_NAMESPACE": "radiant",
    "RADIANT_OPEN_DATA_CATALOG": "odl_catalog",
    "RADIANT_OPEN_DATA_DATABASE": "opendatalake_qa",
    "RADIANT_OPEN_DATA_REF": "latest",
    "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "",
}


def _by_source(conf=None) -> dict[str, dict[str, str]]:
    return {row["source_name"]: row for row in build_open_data_release_rows(conf or _CONF)}


def test_one_row_per_open_data_source():
    rows = build_open_data_release_rows(_CONF)
    expected = len(ICEBERG_OPEN_DATA_CONTRACT_MAPPING) + len(ICEBERG_OPEN_DATA_LEGACY_MAPPING)
    assert len(rows) == expected
    assert len({row["source_name"] for row in rows}) == expected
    assert rows == sorted(rows, key=lambda row: row["source_name"])


def test_a_contract_source_records_the_opendatalake_schema():
    assert _by_source()["clinvar"] == {
        "source_name": "clinvar",
        "table_name": "clinvar_v1",
        "catalog_name": "odl_catalog",
        "database_name": "opendatalake_qa",
        "iceberg_ref": "latest",
        "dataset_version": "",
    }


def test_a_held_back_source_records_the_radiant_schema_it_was_really_read_from():
    """The bug this guards: stamping the ODL catalog and ref onto a source the refresh never read
    there makes the row claim a release that was not used."""
    conf = _CONF | {"RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "gnomad_sv"}
    row = _by_source(conf)["gnomad_sv"]

    assert row["table_name"] == "gnomad_sv"  # the pre-contract name, not gnomad_sv_v1
    assert row["catalog_name"] == "radiant_iceberg_catalog"
    assert row["database_name"] == "radiant"
    # Held back means read without time travel, so there is no ref to record.
    assert row["iceberg_ref"] == ""
    assert row["dataset_version"] == ""

    # Its neighbours are untouched.
    assert _by_source(conf)["clinvar"]["catalog_name"] == "odl_catalog"


def test_holding_everything_back_records_no_opendatalake_row():
    conf = _CONF | {"RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "*"}
    rows = build_open_data_release_rows(conf)

    assert rows, "the sources are still read, just from elsewhere"
    assert {row["catalog_name"] for row in rows} == {"radiant_iceberg_catalog"}
    assert not any(row["iceberg_ref"] for row in rows)


def test_legacy_only_sources_carry_no_ref():
    """`ensembl_gene` and friends have no OpenDataLake contract at all."""
    row = _by_source()["ensembl_gene"]
    assert row["table_name"] == "ensembl_gene"
    assert row["catalog_name"] == "radiant_iceberg_catalog"
    assert row["iceberg_ref"] == ""


@pytest.mark.parametrize(
    ("ref", "dataset_version"),
    [
        # `latest` moves with each publish, so it pins nothing.
        ("latest", ""),
        ("2026-09-01", "2026-09-01"),
    ],
)
def test_dataset_version_is_filled_only_when_the_ref_pins_a_release(ref, dataset_version):
    row = _by_source(_CONF | {"RADIANT_OPEN_DATA_REF": ref})["clinvar"]
    assert row["iceberg_ref"] == ref
    assert row["dataset_version"] == dataset_version


def test_source_name_survives_the_contract_flip():
    """The primary key: the same source held back and not held back must land on one row, not two."""
    held_back = _by_source(_CONF | {"RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "gnomad_sv"})["gnomad_sv"]
    on_contract = _by_source()["gnomad_sv"]

    assert held_back["source_name"] == on_contract["source_name"]
    assert held_back["table_name"] != on_contract["table_name"]
