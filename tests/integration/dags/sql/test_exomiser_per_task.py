"""The Exomiser tables are keyed per task.

A sequencing analysed twice (a duo, then a trio) has two annotation tasks, each with its own Exomiser file. Each task
keeps its own Exomiser rows, and each task's occurrences get their own task's Exomiser values, whether the two tasks
are imported apart or together. The imports run the way `import_part` runs them: the copy step and the insert of the
partition swap, for `raw_exomiser` and for `exomiser`.
"""

import hashlib
import os

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import In

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_TENANT = "test"
_SEQ_ID = 1160101
_DUO = 11601
_TRIO = 11602

_V1, _V2 = 1161001, 1161002

# (task_id, locus_id) -> (gene_combined_score, acmg_classification). The trio has no Exomiser row at V2.
_EXOMISER = {
    (_DUO, _V1): (0.75, "pathogenic"),
    (_DUO, _V2): (0.25, "benign"),
    (_TRIO, _V1): (0.5, "uncertain_significance"),
}


def _locus_hash(locus_id):
    # `raw_exomiser.locus_hash` is generated from the locus, so the variant must carry the same hash.
    return hashlib.sha256(f"1-{locus_id}-A-G".encode()).hexdigest()


def _render(path, mapping, **context):
    with open(os.path.join(_SQL_DIR, path)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping, **context}).rstrip().rstrip(";")


def _swap(cursor, table, part, copy_sql, params, write):
    """The partition swap of `RadiantStarRocksBasePartitionSwapOperator`: copy, write, replace.

    Mirrors `prepare_partition` and `swap_partitions` in `radiant/tasks/starrocks/operator.py`: change both together.
    """
    cursor.execute(f"SHOW PARTITIONS FROM {table} WHERE PartitionName = 'p{part}'")
    if not cursor.fetchall():
        write(table)
        return
    cursor.execute(f"ALTER TABLE {table} DROP TEMPORARY PARTITION IF EXISTS tp{part}")
    cursor.execute(f"ALTER TABLE {table} ADD TEMPORARY PARTITION IF NOT EXISTS tp{part} VALUES IN ('{part}')")
    cursor.execute(f"INSERT INTO {table} TEMPORARY PARTITION (tp{part}) {copy_sql}", params)
    write(f"{table} TEMPORARY PARTITION (tp{part})")
    cursor.execute(f"ALTER TABLE {table} REPLACE PARTITION (p{part}) WITH TEMPORARY PARTITION (tp{part})")


def _import(cursor, mapping, part, task_ids):
    """One `import_part` run of `task_ids`: the Exomiser load into `raw_exomiser`, then `insert_exomiser`."""
    params = {"task_ids": task_ids, "deleted_task_ids": [-1], "tenant_code": _TENANT}
    raw = mapping["starrocks_staging_exomiser"]

    def load(target):
        # The broker load of each task's file, with the columns its `SET` clause fills.
        cursor.executemany(
            f"INSERT INTO {target} (part, seq_id, task_id, tenant_code, id, rank, symbol, moi, variant_score, "
            "gene_combined_score, contributing_variant, chromosome, start, end, reference, alternate, "
            "acmg_classification) "
            "VALUES (%s, %s, %s, %s, %s, 1, 'GENE1', 'AD', %s, %s, true, '1', %s, %s, 'A', 'G', %s)",
            [
                (part, _SEQ_ID, task_id, _TENANT, f"{task_id}-{locus_id}", score, score, locus_id, locus_id, acmg)
                for (task_id, locus_id), (score, acmg) in _EXOMISER.items()
                if task_id in task_ids
            ],
        )

    _swap(
        cursor,
        raw,
        part,
        _render("radiant/staging_exomiser_insert_partition_delta.sql", mapping, table=raw, partition=part),
        params,
        load,
    )

    exomiser = mapping["starrocks_exomiser"]
    insert_sql = _render("radiant/exomiser_insert_partition_delta.sql", mapping, partition=part)
    _swap(
        cursor,
        exomiser,
        part,
        _render("radiant/exomiser_copy_partition.sql", mapping, table=exomiser, partition=part),
        params,
        lambda target: cursor.execute(f"INSERT INTO {target} {insert_sql}", params),
    )


def _occurrence(part, task_id, locus_id):
    return {
        "task_id": task_id,
        "locus": f"1-{locus_id}-A-G",
        "locus_hash": _locus_hash(locus_id),
        "chromosome": "1",
        "start": locus_id,
        "reference": "A",
        "alternate": "G",
        "part": part,
        "seq_id": _SEQ_ID,
        "aliquot": f"SA{_SEQ_ID}",
        "tenant_code": _TENANT,
        "has_alt": True,
        "phased": False,
    }


@pytest.fixture
def exomiser_tables(starrocks_session, iceberg_client, setup_iceberg_namespace, radiant_mapping):
    tables = ("snv_tmp_variant", "staging_exomiser", "exomiser", "germline_snv_comphet", "germline_snv_occurrence")
    with starrocks_session.cursor() as cursor:
        for table in tables:
            cursor.execute(_render(f"radiant/init/{table}_create_table.sql", radiant_mapping))
            cursor.execute(f"TRUNCATE TABLE {radiant_mapping[f'starrocks_{table}']}")
        cursor.executemany(
            f"INSERT INTO {radiant_mapping['starrocks_snv_tmp_variant']} (locus_id, locus_hash) VALUES (%s, %s)",
            [(locus_id, _locus_hash(locus_id)) for locus_id in (_V1, _V2)],
        )

    iceberg_table = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_occurrence")
    yield iceberg_table

    iceberg_table.delete(In("task_id", [_DUO, _TRIO]))
    with starrocks_session.cursor() as cursor:
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {radiant_mapping[f'starrocks_{table}']}")


@pytest.mark.parametrize(
    "part, runs",
    [
        pytest.param(1161, [[_DUO], [_TRIO]], id="apart"),
        pytest.param(1162, [[_DUO, _TRIO]], id="together"),
    ],
)
def test_each_task_keeps_its_own_exomiser_values(starrocks_session, radiant_mapping, exomiser_tables, part, runs):
    with starrocks_session.cursor() as cursor:
        for task_ids in runs:
            _import(cursor, radiant_mapping, part, task_ids)

        cursor.execute(f"SELECT task_id, count(1) FROM {radiant_mapping['starrocks_staging_exomiser']} GROUP BY 1")
        assert dict(cursor.fetchall()) == {_DUO: 2, _TRIO: 1}

        cursor.execute(
            f"SELECT task_id, locus_id, variant_rank, gene_combined_score, acmg_classification "
            f"FROM {radiant_mapping['starrocks_exomiser']} WHERE part = {part} AND seq_id = {_SEQ_ID}"
        )
        # Ranked per task: the duo and the trio both rank their own row first at V1.
        assert {(t, locus): (rank, score, acmg) for t, locus, rank, score, acmg in cursor.fetchall()} == {
            (task_id, locus_id): (1, score, acmg) for (task_id, locus_id), (score, acmg) in _EXOMISER.items()
        }

    exomiser_tables.append(
        pa.Table.from_pylist(
            [_occurrence(part, task_id, locus_id) for task_id in (_DUO, _TRIO) for locus_id in (_V1, _V2)],
            schema=exomiser_tables.schema().as_arrow(),
        )
    )
    target = radiant_mapping["starrocks_germline_snv_occurrence"]
    insert_sql = _render("radiant/germline_snv_occurrence_insert_partition_delta.sql", radiant_mapping, partition=part)
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping['iceberg_germline_snv_occurrence']}")
        cursor.execute(f"INSERT INTO {target} {insert_sql}", {"task_ids": [_DUO, _TRIO], "tenant_code": _TENANT})
        cursor.execute(
            f"SELECT task_id, locus_id, exomiser_gene_combined_score, exomiser_acmg_classification "
            f"FROM {target} WHERE part = {part}"
        )
        occurrences = {(t, locus): (score, acmg) for t, locus, score, acmg in cursor.fetchall()}

    assert occurrences == {
        (_DUO, _V1): (0.75, "pathogenic"),
        (_DUO, _V2): (0.25, "benign"),
        (_TRIO, _V1): (0.5, "uncertain_significance"),
        # The duo's row at V2 must not reach the trio.
        (_TRIO, _V2): (None, None),
    }
