"""Which Iceberg tables `import-open-data` refreshes, and in which catalog (SJRA-1811).

`REFRESH EXTERNAL TABLE` has to name the table the statements will actually read. Refreshing the wrong
catalog is worse than not refreshing: the run looks clean and the loads still read a stale cache.
"""

import pytest

from radiant.tasks.data.open_data import list_iceberg_source_tables
from radiant.tasks.data.radiant_tables import (
    ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
    ICEBERG_OPEN_DATA_LEGACY_MAPPING,
)

_CONF = {
    "RADIANT_ICEBERG_CATALOG": "radiant_iceberg_catalog",
    "RADIANT_ICEBERG_NAMESPACE": "radiant",
    "RADIANT_OPEN_DATA_CATALOG": "odl_catalog",
    "RADIANT_OPEN_DATA_DATABASE": "opendatalake_qa",
}

_ODL = "odl_catalog.opendatalake_qa"
_LEGACY = "radiant_iceberg_catalog.radiant"


def test_every_source_is_refreshed_exactly_once():
    tables = list_iceberg_source_tables(_CONF)
    assert len(tables) == len(ICEBERG_OPEN_DATA_CONTRACT_MAPPING) + len(ICEBERG_OPEN_DATA_LEGACY_MAPPING)
    assert len(set(tables)) == len(tables)
    assert tables == sorted(tables)


def test_contract_tables_are_refreshed_in_the_opendatalake_catalog():
    tables = list_iceberg_source_tables(_CONF)
    assert f"{_ODL}.clinvar_v1" in tables
    # The three with no OpenDataLake contract never move.
    assert f"{_LEGACY}.ensembl_gene" in tables


def test_names_carry_no_time_travel_clause():
    """`get_iceberg_open_data_mapping` composes `VERSION AS OF '<ref>'` into its values, and
    `REFRESH EXTERNAL TABLE` will not accept a relation carrying one."""
    assert not any("VERSION AS OF" in table for table in list_iceberg_source_tables(_CONF))


@pytest.mark.parametrize(
    ("held_back", "expected"),
    [("clinvar", f"{_LEGACY}.clinvar"), ("gnomad_joint", f"{_LEGACY}.gnomad_genomes_v3")],
)
def test_a_held_back_source_is_refreshed_where_it_is_actually_read(held_back, expected):
    tables = list_iceberg_source_tables({**_CONF, "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": held_back})
    assert expected in tables
    assert not any(table.startswith(f"{_ODL}.{held_back}") for table in tables)
