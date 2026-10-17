"""RAD-39 -- the compound-het pairs resolve both locus hashes to `locus_id` through `snv__tmp_variant`.

A hash that does not resolve drops the pair silently, so the values are asserted here, with a row on each
side of the tenant, task and part filters.
"""

import os

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

# Own part and task ids so the rows are addressable: the StarRocks table is partitioned by `part`, the
# Iceberg teardown deletes on `task_id`.
_PART = 3901
_OTHER_PART = 3902
_TASK_ID = 39001
_OTHER_TASK_ID = 39002
_TENANT = "test"
_OTHER_TENANT = "other"
_SEQ_ID = 3900101

# locus_hash -> locus_id, the rows of `snv__tmp_variant`.
_V1, _V2, _V3, _UNKNOWN = "hash-v1", "hash-v2", "hash-v3", "hash-not-in-tmp-variant"
_LOCUS_IDS = {_V1: 3901001, _V2: 3901002, _V3: 3901003}


def _row(locus_hash, partner_locus_hash, *, symbol="GENE1", part=_PART, task_id=_TASK_ID, tenant_code=_TENANT):
    return {
        "task_id": task_id,
        "locus_hash": locus_hash,
        "part": part,
        "seq_id": _SEQ_ID,
        "tenant_code": tenant_code,
        "partner_locus_hash": partner_locus_hash,
        "symbol": symbol,
    }


_ICEBERG_ROWS = [
    # V1 is paired with V2 and with V3, each pair once per direction.
    _row(_V1, _V2),
    _row(_V2, _V1),
    _row(_V1, _V3),
    _row(_V3, _V1),
    # The same variants in a second gene.
    _row(_V1, _V2, symbol="GENE2"),
    # A partner missing from tmp_variant: dropped, not loaded with a NULL id.
    _row(_V2, _UNKNOWN),
    # Filtered out: other tenant, other task, other part.
    _row(_V1, _V2, tenant_code=_OTHER_TENANT),
    _row(_V1, _V2, task_id=_OTHER_TASK_ID),
    _row(_V1, _V2, part=_OTHER_PART),
]


def _render(path, mapping, **context):
    with open(os.path.join(_SQL_DIR, path)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping, **context})


@pytest.fixture
def seeded_comphet(starrocks_session, iceberg_client, setup_iceberg_namespace, radiant_mapping):
    with starrocks_session.cursor() as cursor:
        for table in ("snv_tmp_variant", "germline_snv_comphet"):
            cursor.execute(_render(f"radiant/init/{table}_create_table.sql", radiant_mapping))
            cursor.execute(f"TRUNCATE TABLE {radiant_mapping[f'starrocks_{table}']}")
        cursor.executemany(
            f"INSERT INTO {radiant_mapping['starrocks_snv_tmp_variant']} (locus_id, locus_hash) VALUES (%s, %s)",
            [(locus_id, locus_hash) for locus_hash, locus_id in _LOCUS_IDS.items()],
        )

    iceberg_table = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_comphet")
    # The table's own schema, not the module's: Iceberg renumbers field ids on create.
    iceberg_table.append(pa.Table.from_pylist(_ICEBERG_ROWS, schema=iceberg_table.schema().as_arrow()))
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping['iceberg_germline_snv_comphet']}")

    yield
    for task_id in (_TASK_ID, _OTHER_TASK_ID):
        iceberg_table.delete(EqualTo("task_id", task_id))


def test_comphet_insert_resolves_both_locus_ids(seeded_comphet, starrocks_session, radiant_mapping):
    target = radiant_mapping["starrocks_germline_snv_comphet"]
    # Positional INSERT INTO, the way `RadiantStarRocksPartitionSwapOperator` runs it.
    rendered_sql = _render("radiant/germline_snv_comphet_insert_partition_delta.sql", radiant_mapping, partition=_PART)
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {target} {rendered_sql.rstrip().rstrip(';')}",
            {"task_ids": [_TASK_ID], "tenant_code": _TENANT},
        )
        cursor.execute(f"SELECT part, seq_id, task_id, locus_id, partner_locus_id, symbol FROM {target}")
        rows = sorted(cursor.fetchall())

    v1, v2, v3 = _LOCUS_IDS[_V1], _LOCUS_IDS[_V2], _LOCUS_IDS[_V3]
    assert rows == sorted(
        [
            (_PART, _SEQ_ID, _TASK_ID, v1, v2, "GENE1"),
            (_PART, _SEQ_ID, _TASK_ID, v2, v1, "GENE1"),
            (_PART, _SEQ_ID, _TASK_ID, v1, v3, "GENE1"),
            (_PART, _SEQ_ID, _TASK_ID, v3, v1, "GENE1"),
            (_PART, _SEQ_ID, _TASK_ID, v1, v2, "GENE2"),
        ]
    )
