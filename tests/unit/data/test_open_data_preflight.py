"""Preflight existence checks for the open-data refresh (SJRA-1811).

The reference load is one serial chain, so it surfaces one missing table per run. These cover the
thing that makes bringing an environment up bearable: every gap reported at once, grouped by cause.
"""

from unittest.mock import patch

import pytest

from radiant.tasks.data import open_data
from radiant.tasks.data.radiant_tables import (
    ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
    ICEBERG_OPEN_DATA_LEGACY_MAPPING,
    ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING,
    STARROCKS_OPEN_DATA_MAPPING,
)

_CONF = {
    "RADIANT_TABLES_DATABASE": "radiant",
    "RADIANT_ICEBERG_CATALOG": "radiant_iceberg_catalog",
    "RADIANT_ICEBERG_NAMESPACE": "radiant",
    "RADIANT_OPEN_DATA_CATALOG": "odl_catalog",
    "RADIANT_OPEN_DATA_DATABASE": "opendatalake_qa",
    "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "",
}

_ODL = "odl_catalog.opendatalake_qa"
_LEGACY = "radiant_iceberg_catalog.radiant"


@pytest.fixture
def tables_in():
    """Stub `SHOW TABLES FROM <schema>`, keyed by schema."""
    with patch.object(open_data, "_tables_in") as mock:
        yield mock


def _everything_present():
    return {
        _ODL: set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING.values()),
        _LEGACY: set(ICEBERG_OPEN_DATA_LEGACY_MAPPING.values()),
        "radiant": set(STARROCKS_OPEN_DATA_MAPPING.values()),
    }


def test_nothing_missing_returns_empty(tables_in):
    present = _everything_present()
    tables_in.side_effect = lambda schema: present[schema]
    assert open_data.list_missing_open_data_tables(_CONF) == {}


def test_reports_every_gap_at_once_not_just_the_first(tables_in):
    """The whole point: the serial reference load would surface these one run at a time."""
    present = _everything_present()
    present[_ODL] = present[_ODL] - {"omim_v1", "topmed_bravo_v1"}
    present[_LEGACY] = present[_LEGACY] - {"cosmic_gene_set"}
    present["radiant"] = present["radiant"] - {"cytoband"}
    tables_in.side_effect = lambda schema: present[schema]

    missing = open_data.list_missing_open_data_tables(_CONF)
    assert missing == {
        "OpenDataLake contract tables": [f"{_ODL}.omim_v1", f"{_ODL}.topmed_bravo_v1"],
        "Legacy Iceberg tables": [f"{_LEGACY}.cosmic_gene_set"],
        "StarRocks target tables": ["radiant.cytoband"],
    }


def test_groups_are_dropped_when_they_are_complete(tables_in):
    present = _everything_present()
    present[_ODL] = present[_ODL] - {"clinvar_v1"}
    tables_in.side_effect = lambda schema: present[schema]

    missing = open_data.list_missing_open_data_tables(_CONF)
    assert list(missing) == ["OpenDataLake contract tables"]


def test_contract_tables_are_looked_up_by_their_bare_name(tables_in):
    """`get_iceberg_open_data_mapping` composes `VERSION AS OF '<ref>'` into its values; SHOW TABLES
    returns bare names, so the check must not be built from the composed relation."""
    present = _everything_present()
    tables_in.side_effect = lambda schema: present[schema]
    open_data.list_missing_open_data_tables(_CONF)

    assert [call.args[0] for call in tables_in.call_args_list] == [_ODL, _LEGACY, "radiant"]


def test_held_back_sources_are_looked_for_under_their_pre_contract_name(tables_in):
    """The `*` default, and any partial hold-back: the refresh reads those sources in the Radiant
    catalog, so a check against the contract names would report a whole catalog as missing."""
    conf = _CONF | {"RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "*"}
    present = {
        _LEGACY: set(ICEBERG_OPEN_DATA_LEGACY_MAPPING.values()) | set(ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING.values()),
        "radiant": set(STARROCKS_OPEN_DATA_MAPPING.values()),
    }
    tables_in.side_effect = lambda schema: present[schema]

    assert open_data.list_missing_open_data_tables(conf) == {}
    # The ODL catalog is not read at all -- an environment that holds everything back need not have one.
    assert [call.args[0] for call in tables_in.call_args_list] == [_LEGACY, "radiant"]


def test_an_unreachable_catalog_propagates_rather_than_reading_as_missing(tables_in):
    # "the catalog is not configured" needs a different fix from "the release is not published yet".
    tables_in.side_effect = RuntimeError("Unknown catalog 'odl_catalog'")
    with pytest.raises(RuntimeError, match="Unknown catalog"):
        open_data.list_missing_open_data_tables(_CONF)


def test_message_lists_every_table_under_its_cause():
    message = open_data.format_missing_tables(
        {
            "OpenDataLake contract tables": [f"{_ODL}.omim_v1", f"{_ODL}.topmed_bravo_v1"],
            "StarRocks target tables": ["radiant.cytoband"],
        }
    )
    assert message.startswith("3 table(s) the open-data refresh needs are missing:")
    assert "  OpenDataLake contract tables:" in message
    assert f"    - {_ODL}.omim_v1" in message
    assert "    - radiant.cytoband" in message


def test_iceberg_source_tables_are_bare_qualified_names(tables_in):
    """Feeds `REFRESH EXTERNAL TABLE`, which rejects a relation carrying a temporal clause -- so these
    must not come from `get_iceberg_open_data_mapping`, whose values embed `VERSION AS OF '<ref>'`."""
    tables = open_data.list_iceberg_source_tables(_CONF)

    assert f"{_ODL}.clinvar_v1" in tables
    assert f"{_LEGACY}.ensembl_gene" in tables
    assert not any("VERSION AS OF" in table for table in tables)
    # Iceberg sources only: the StarRocks targets are not external tables and cannot be refreshed.
    assert not any(table.startswith("radiant.") for table in tables)
    assert len(tables) == len(ICEBERG_OPEN_DATA_CONTRACT_MAPPING) + len(ICEBERG_OPEN_DATA_LEGACY_MAPPING)
    assert tables == sorted(tables)
