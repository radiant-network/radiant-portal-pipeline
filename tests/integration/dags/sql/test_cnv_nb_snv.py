"""SJRA-1811 -- `nb_snv` counts the part's SNV occurrences, coordinates joined from `snv__variant`.

A mismatched join key fails silently: no rows, every `nb_snv` NULL, indistinguishable from a part with no
SNVs. `EXPLAIN` plans that happily, so the values are asserted here, with a row on each side of the
interval, chromosome, sample and part boundaries.
"""

import os

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

# Own part and task_id so the rows are addressable: the tables are partitioned by `part`, the Iceberg
# teardown deletes on `task_id`.
_PART = 1888
_OTHER_PART = 1889
_TASK_ID = 18110
# Same sample and locus under a second task -- what makes COUNT(1) and COUNT(DISTINCT locus_id) differ.
_SECOND_TASK_ID = 18111
_TENANT = "test"

_SEQ_ID = 1811001
_OTHER_SEQ_ID = 1811002

# The CNV segment every assertion is about: chromosome 1, 1000-2000 inclusive.
_CNV_START = 1000
_CNV_END = 2000

# One locus per predicate under test.
_INSIDE = 1811101
_AT_START = 1811102  # pins `>=` rather than `>`
_AT_END = 1811103  # pins `<=` rather than `<`
_TWO_TASKS = 1811104
_BEFORE = 1811105
_AFTER = 1811106
_OTHER_CHROMOSOME = 1811107
_OTHER_SAMPLE = 1811108
_WRONG_PART = 1811109
# An occurrence whose locus is absent from `snv__variant`. Routine, not exotic: that table is restricted to
# loci which reached a frequency table, so anything failing gq/filter/ad_alt is missing and drops out of the
# INNER JOIN. This is what makes nb_snv a count of quality-passing SNVs.
_NO_VARIANT_ROW = 1811110

# (locus_id, chromosome, start) -- the only source of coordinates; the occurrence row has none.
_VARIANTS = [
    (_INSIDE, "1", 1500),
    (_AT_START, "1", _CNV_START),
    (_AT_END, "1", _CNV_END),
    (_TWO_TASKS, "1", 1600),
    (_BEFORE, "1", _CNV_START - 1),
    (_AFTER, "1", _CNV_END + 1),
    (_OTHER_CHROMOSOME, "2", 1500),
    (_OTHER_SAMPLE, "1", 1500),
    (_WRONG_PART, "1", 1500),
]

# (part, seq_id, task_id, locus_id)
_SNV_OCCURRENCES = [
    (_PART, _SEQ_ID, _TASK_ID, _INSIDE),
    (_PART, _SEQ_ID, _TASK_ID, _AT_START),
    (_PART, _SEQ_ID, _TASK_ID, _AT_END),
    (_PART, _SEQ_ID, _TASK_ID, _TWO_TASKS),
    (_PART, _SEQ_ID, _SECOND_TASK_ID, _TWO_TASKS),
    (_PART, _SEQ_ID, _TASK_ID, _BEFORE),
    (_PART, _SEQ_ID, _TASK_ID, _AFTER),
    (_PART, _SEQ_ID, _TASK_ID, _OTHER_CHROMOSOME),
    (_PART, _SEQ_ID, _TASK_ID, _NO_VARIANT_ROW),
    (_PART, _OTHER_SEQ_ID, _TASK_ID, _OTHER_SAMPLE),
    (_OTHER_PART, _SEQ_ID, _TASK_ID, _WRONG_PART),
]

# What survives every predicate, for the chromosome-1 segment of `_SEQ_ID`.
_EXPECTED_NB_SNV = 4

_SEGMENT = "cnv-chr1"
_SEGMENT_OTHER_SAMPLE = "cnv-chr1-other-sample"
_SEGMENT_NO_SNV = "cnv-chr3-no-snv"


def _cnv_row(*, name, seq_id, chromosome, start=_CNV_START, end=_CNV_END):
    """One Iceberg CNV segment, every required field of the schema filled.

    `type` is GAIN, not UNKNOWN: `GET_CNV_ID` returns NULL for a type it cannot encode, and `cnv_id` is
    NOT NULL on the target table.
    """
    return {
        "part": _PART,
        "seq_id": seq_id,
        "tenant_code": _TENANT,
        "task_id": _TASK_ID,
        "aliquot": f"SA{seq_id}",
        "chromosome": chromosome,
        "alternate": "<DUP>",
        "start": start,
        "end": end,
        "type": "GAIN",
        "length": end - start,
        "name": name,
    }


_CNV_ROWS = [
    _cnv_row(name=_SEGMENT, seq_id=_SEQ_ID, chromosome="1"),
    # Same coordinates, different sample: scoping asserted in both directions.
    _cnv_row(name=_SEGMENT_OTHER_SAMPLE, seq_id=_OTHER_SEQ_ID, chromosome="1"),
    # Nothing overlaps it. `snv` is LEFT JOINed, so the segment still loads with a NULL count.
    _cnv_row(name=_SEGMENT_NO_SNV, seq_id=_SEQ_ID, chromosome="3"),
]


def _create_table(starrocks_session, sql_subdir, table_name, mapping, truncate):
    with open(os.path.join(_SQL_DIR, sql_subdir, "init", f"{table_name}_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"mapping": mapping})

    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {mapping[f'starrocks_{table_name}']};")


def _insert_rows(starrocks_session, table, columns, rows):
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    with starrocks_session.cursor() as cursor:
        cursor.executemany(sql, rows)


def _create_cnv_id_udf(starrocks_session, mapping):
    with open(os.path.join(_SQL_DIR, "radiant", "init", "cnv_id_udf.sql")) as f_in:
        udf_sql = jinja2.Template(f_in.read()).render(
            {"mapping": mapping, "params": {"udf_release_version": "v2.0.0"}}
        )
    with starrocks_session.cursor() as cursor:
        cursor.execute(udf_sql)


def _seed(starrocks_session, iceberg_client, namespace, mapping, *, flavour):
    """Seed one flavour's tables and return its Iceberg table, ready for teardown.

    Germline and somatic differ only in the occurrence table's sample column and in which Iceberg CNV
    table holds the segments.
    """
    occurrence_table = f"{flavour}_snv_occurrence"
    sample_column = "seq_id" if flavour == "germline" else "tumor_seq_id"

    # Truncated because this test seeds them in full. `cytoband` and `ensembl_gene` are only created --
    # they feed columns nothing here asserts.
    for table in (f"{flavour}_cnv_occurrence", occurrence_table, "snv_variant"):
        _create_table(starrocks_session, "radiant", table, mapping, truncate=True)
    for table in ("cytoband", "ensembl_gene"):
        _create_table(starrocks_session, "open_data", table, mapping, truncate=False)

    _create_cnv_id_udf(starrocks_session, mapping)

    _insert_rows(
        starrocks_session,
        mapping["starrocks_snv_variant"],
        ("locus_id", "chromosome", "start"),
        _VARIANTS,
    )

    occurrence_columns = ["part", sample_column, "task_id", "locus_id"]
    occurrence_rows = [(part, seq_id, task_id, locus_id) for part, seq_id, task_id, locus_id in _SNV_OCCURRENCES]
    if flavour == "germline":
        # `phased` is NOT NULL on the germline occurrence table only.
        occurrence_columns.append("phased")
        occurrence_rows = [(*row, False) for row in occurrence_rows]

    _insert_rows(
        starrocks_session,
        mapping[f"starrocks_{occurrence_table}"],
        occurrence_columns,
        occurrence_rows,
    )

    iceberg_table = iceberg_client.load_table(f"{namespace}.{flavour}_cnv_occurrence")
    # The table's own schema, not the module's: Iceberg renumbers field ids on create, and the append is
    # validated by id.
    iceberg_table.append(pa.Table.from_pylist(_CNV_ROWS, schema=iceberg_table.schema().as_arrow()))

    # StarRocks caches Iceberg metadata, which `test_queries.py` may already have populated by EXPLAINing
    # this table; without the refresh the insert reads a pre-append snapshot.
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {mapping[f'iceberg_{flavour}_cnv_occurrence']}")

    return iceberg_table


def _run_cnv_insert(starrocks_session, mapping, flavour):
    """Run the delta insert the way `RadiantStarRocksPartitionSwapOperator` does: positional INSERT INTO."""
    sql_file = os.path.join(_SQL_DIR, "radiant", f"{flavour}_cnv_occurrence_insert_partition_delta.sql")
    with open(sql_file) as f_in:
        rendered_sql = jinja2.Template(f_in.read()).render({"mapping": mapping, "partition": _PART})

    target = mapping[f"starrocks_{flavour}_cnv_occurrence"]
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {target} {rendered_sql.rstrip().rstrip(';')}",
            {"seq_ids": [_SEQ_ID, _OTHER_SEQ_ID], "tenant_code": _TENANT},
        )


def _nb_snv_by_segment(starrocks_session, mapping, flavour):
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"SELECT name, nb_snv FROM {mapping[f'starrocks_{flavour}_cnv_occurrence']}")
        return dict(cursor.fetchall())


@pytest.fixture(params=["germline", "somatic"])
def seeded_cnv(
    request,
    starrocks_session,
    iceberg_client,
    setup_iceberg_namespace,
    # For `gnomad_sv`, which the CNV statement LEFT JOINs and which exists only in Iceberg.
    open_data_iceberg_tables,
    radiant_mapping,
):
    flavour = request.param
    iceberg_table = _seed(
        starrocks_session,
        iceberg_client,
        setup_iceberg_namespace,
        radiant_mapping,
        flavour=flavour,
    )
    yield flavour
    # The Iceberg CNV tables are shared with the VCF tests, which assert unfiltered row counts on them.
    iceberg_table.delete(EqualTo("task_id", _TASK_ID))


def test_nb_snv_counts_the_overlapping_snvs_of_the_same_sample(seeded_cnv, starrocks_session, radiant_mapping):
    flavour = seeded_cnv
    _run_cnv_insert(starrocks_session, radiant_mapping, flavour)

    nb_snv = _nb_snv_by_segment(starrocks_session, radiant_mapping, flavour)
    assert set(nb_snv) == {_SEGMENT, _SEGMENT_OTHER_SAMPLE, _SEGMENT_NO_SNV}

    # The whole point: exact and non-NULL. NULL means the occurrence -> staging-variant join matched
    # nothing, the silent failure `EXPLAIN` cannot catch.
    assert nb_snv[_SEGMENT] == _EXPECTED_NB_SNV

    # Scoped per sample in both directions.
    assert nb_snv[_SEGMENT_OTHER_SAMPLE] == 1

    # LEFT JOINed: the segment loads even with nothing to count.
    assert nb_snv[_SEGMENT_NO_SNV] is None


def test_nb_snv_counts_a_locus_once_across_tasks(seeded_cnv, starrocks_session, radiant_mapping):
    """`COUNT(DISTINCT locus_id)`, not `COUNT(1)`.

    The occurrence tables are keyed on `task_id`, so a sample analysed twice -- routine on the somatic
    side -- holds the same locus twice and a row count reports it as two SNVs. `_TWO_TASKS` is that
    locus, so the two counts differ by exactly one.
    """
    flavour = seeded_cnv
    _run_cnv_insert(starrocks_session, radiant_mapping, flavour)

    nb_snv = _nb_snv_by_segment(starrocks_session, radiant_mapping, flavour)
    assert nb_snv[_SEGMENT] == _EXPECTED_NB_SNV, (
        f"expected {_EXPECTED_NB_SNV} distinct loci; {_EXPECTED_NB_SNV + 1} means the duplicate "
        f"`task_id` row for locus {_TWO_TASKS} was counted twice"
    )
