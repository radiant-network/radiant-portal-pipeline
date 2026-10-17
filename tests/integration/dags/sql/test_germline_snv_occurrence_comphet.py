"""RAD-88 -- the occurrence insert fills `has_comphet` and `comphet_genes` from `germline__snv__comphet`.

The count of a gene must equal the number of partners the portal lists from the pair table, so the values
are asserted here, with a sample and a task on each side of the join.
"""

import json
import os

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_PART = 3911
_TASK_ID = 39101
_OTHER_TASK_ID = 39102
_TENANT = "test"
_SEQ_ID = 3910101
_OTHER_SEQ_ID = 3910102

# locus_hash -> locus_id
_V1, _V2, _V3, _V4 = 3911001, 3911002, 3911003, 3911004
_HASHES = {f"hash-{locus_id}": locus_id for locus_id in (_V1, _V2, _V3, _V4)}

# (seq_id, task_id, locus_id, partner_locus_id, symbol)
_PAIRS = [
    # The spec example: V1 paired with V2 and V3 in BRCA2, each pair once per direction.
    (_SEQ_ID, _TASK_ID, _V1, _V2, "BRCA2"),
    (_SEQ_ID, _TASK_ID, _V2, _V1, "BRCA2"),
    (_SEQ_ID, _TASK_ID, _V1, _V3, "BRCA2"),
    (_SEQ_ID, _TASK_ID, _V3, _V1, "BRCA2"),
    # V1 and V2 also overlap a second gene.
    (_SEQ_ID, _TASK_ID, _V1, _V2, "GENE2"),
    (_SEQ_ID, _TASK_ID, _V2, _V1, "GENE2"),
    # Another task of the same sample: not counted.
    (_SEQ_ID, _OTHER_TASK_ID, _V4, _V1, "BRCA2"),
]


def _occurrence(seq_id, locus_id):
    return {
        "task_id": _TASK_ID,
        "locus": f"1-{locus_id}-A-G",
        "locus_hash": f"hash-{locus_id}",
        "chromosome": "1",
        "start": locus_id,
        "reference": "A",
        "alternate": "G",
        "part": _PART,
        "seq_id": seq_id,
        "aliquot": f"SA{seq_id}",
        "tenant_code": _TENANT,
        "has_alt": True,
        "phased": False,
    }


_OCCURRENCES = [
    *(_occurrence(_SEQ_ID, locus_id) for locus_id in (_V1, _V2, _V3, _V4)),
    # Same locus as a paired variant, other sample with no pair.
    _occurrence(_OTHER_SEQ_ID, _V1),
]


def _render(path, mapping, **context):
    with open(os.path.join(_SQL_DIR, path)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping, **context})


@pytest.fixture
def summary(starrocks_session, iceberg_client, setup_iceberg_namespace, radiant_mapping):
    """Seed the pairs and the Iceberg occurrences, run the occurrence insert, return its rows by sample/locus."""
    with starrocks_session.cursor() as cursor:
        for table in ("snv_tmp_variant", "exomiser", "germline_snv_comphet", "germline_snv_occurrence"):
            cursor.execute(_render(f"radiant/init/{table}_create_table.sql", radiant_mapping))
            cursor.execute(f"TRUNCATE TABLE {radiant_mapping[f'starrocks_{table}']}")
        cursor.executemany(
            f"INSERT INTO {radiant_mapping['starrocks_snv_tmp_variant']} (locus_id, locus_hash) VALUES (%s, %s)",
            [(locus_id, locus_hash) for locus_hash, locus_id in _HASHES.items()],
        )
        cursor.executemany(
            f"INSERT INTO {radiant_mapping['starrocks_germline_snv_comphet']} "
            "(part, seq_id, task_id, locus_id, partner_locus_id, symbol) VALUES (%s, %s, %s, %s, %s, %s)",
            [(_PART, *pair) for pair in _PAIRS],
        )

    iceberg_table = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_occurrence")
    iceberg_table.append(pa.Table.from_pylist(_OCCURRENCES, schema=iceberg_table.schema().as_arrow()))

    target = radiant_mapping["starrocks_germline_snv_occurrence"]
    rendered_sql = _render(
        "radiant/germline_snv_occurrence_insert_partition_delta.sql", radiant_mapping, partition=_PART
    )
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping['iceberg_germline_snv_occurrence']}")
        # Positional INSERT INTO, the way `RadiantStarRocksPartitionSwapOperator` runs it.
        cursor.execute(
            f"INSERT INTO {target} {rendered_sql.rstrip().rstrip(';')}",
            {"task_ids": [_TASK_ID], "tenant_code": _TENANT},
        )
        cursor.execute(f"SELECT seq_id, locus_id, has_comphet, comphet_genes FROM {target} WHERE part = {_PART}")
        rows = {
            (seq_id, locus_id): (has_comphet, json.loads(genes) if genes else None)
            for seq_id, locus_id, has_comphet, genes in cursor.fetchall()
        }

    yield rows
    iceberg_table.delete(EqualTo("task_id", _TASK_ID))


def test_summary_counts_the_partners_per_gene(summary):
    assert summary == {
        (_SEQ_ID, _V1): (1, [{"symbol": "BRCA2", "nb": 2}, {"symbol": "GENE2", "nb": 1}]),
        (_SEQ_ID, _V2): (1, [{"symbol": "BRCA2", "nb": 1}, {"symbol": "GENE2", "nb": 1}]),
        (_SEQ_ID, _V3): (1, [{"symbol": "BRCA2", "nb": 1}]),
        # Only paired in another task.
        (_SEQ_ID, _V4): (0, None),
        # Same locus as V1, other sample.
        (_OTHER_SEQ_ID, _V1): (0, None),
    }
