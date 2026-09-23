"""Tenant and partition discovery (SJRA-1811 §5).

`import_part` is handed the one part it processes; the re-annotation DAG has to discover every part
that exists, and the fan-out it builds from these is what decides how many mapped tasks run.
"""

from unittest.mock import patch

import pytest

from radiant.tasks.data import tenants

_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}


@pytest.fixture
def query_rows():
    with patch.object(tenants, "_query") as mock:
        yield mock


def test_list_all_parts_is_sorted_and_deduplicated(query_rows):
    query_rows.return_value = [(3,), (1,), (3,), (0,)]
    assert tenants.list_all_parts(_CONF) == [0, 1, 3]


def test_list_all_parts_reads_the_staging_table(query_rows):
    query_rows.return_value = []
    tenants.list_all_parts(_CONF)
    sql = query_rows.call_args.args[0]
    assert "radiant.staging_sequencing_experiment" in sql
    # A part with no experiments has nothing to re-annotate.
    assert "part IS NOT NULL" in sql


def test_list_all_parts_coerces_to_int(query_rows):
    # The driver can hand back a Decimal for an INT column; the value goes on to build a SQL predicate.
    query_rows.return_value = [("2",), (1,)]
    assert tenants.list_all_parts(_CONF) == [1, 2]


def test_list_tenant_parts_returns_only_the_pairs_that_exist(query_rows):
    query_rows.return_value = [("SJ", 3), ("CHOP", 1), ("CHOP", 0), ("SJ", 3)]
    assert tenants.list_tenant_parts(_CONF) == [
        {"tenant_code": "CHOP", "part": 0},
        {"tenant_code": "CHOP", "part": 1},
        {"tenant_code": "SJ", "part": 3},
    ]


def test_list_all_tenants_drops_nulls(query_rows):
    query_rows.return_value = [("CHOP",), (None,), ("SJ",)]
    assert tenants.list_all_tenants(_CONF) == ["CHOP", "SJ"]
