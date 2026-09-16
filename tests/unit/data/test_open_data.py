"""Which Iceberg tables `import-open-data` refreshes, and in which catalog (SJRA-1811).

`REFRESH EXTERNAL TABLE` has to name the table the statements will actually read. Refreshing the wrong
catalog is worse than not refreshing: the run looks clean and the loads still read a stale cache.
"""

import re

import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.open_data import list_iceberg_source_tables
from radiant.tasks.data.radiant_tables import (
    ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
    ICEBERG_OPEN_DATA_LEGACY_MAPPING,
    IS_CONTRACT_SUFFIX,
)

_CONF = {
    "RADIANT_ICEBERG_CATALOG": "radiant_iceberg_catalog",
    "RADIANT_ICEBERG_NAMESPACE": "radiant",
    "RADIANT_OPEN_DATA_CATALOG": "odl_catalog",
    "RADIANT_OPEN_DATA_DATABASE": "opendatalake_qa",
    "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "",
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


def _keys_refreshed_by_import_part() -> set[str]:
    """The `keys=` literal in `import_part`'s get_tables_to_refresh, read from the source so the
    assertion below stays tied to the DAG rather than to a copy of its list."""
    src = (DAGS_DIR / "import_part.py").read_text()
    call = re.search(r"list_iceberg_source_tables\([^)]*keys=\[([^\]]*)\]", src)
    assert call, "get_tables_to_refresh no longer calls list_iceberg_source_tables with a keys= list"
    return set(re.findall(r'"(\w+)"', call.group(1)))


def test_import_part_refreshes_every_open_data_source_its_sql_reads():
    open_data_keys = set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING) | set(ICEBERG_OPEN_DATA_LEGACY_MAPPING)
    referenced = set()
    for path in (DAGS_DIR / "sql" / "radiant").rglob("*.sql"):
        for key in re.findall(r"mapping\.(iceberg_\w+)", path.read_text()):
            referenced.add(key.removesuffix(IS_CONTRACT_SUFFIX))

    assert referenced & open_data_keys == _keys_refreshed_by_import_part(), (
        "sql/radiant reads a different set of open-data sources than import_part refreshes -- "
        "update the keys= list in its get_tables_to_refresh task"
    )


def test_those_sources_resolve_like_any_other():
    keys = ["iceberg_gnomad_sv"]
    assert list_iceberg_source_tables(_CONF, keys=keys) == [f"{_ODL}.gnomad_sv_v1"]
    held_back = {**_CONF, "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "gnomad_sv"}
    assert list_iceberg_source_tables(held_back, keys=keys) == [f"{_LEGACY}.gnomad_sv"]


def test_an_unknown_key_is_rejected_rather_than_dropped():
    # Silently dropping it would leave that source unrefreshed, which is the bug this guards against.
    with pytest.raises(KeyError, match="iceberg_nope"):
        list_iceberg_source_tables(_CONF, keys=["iceberg_gnomad_sv", "iceberg_nope"])
