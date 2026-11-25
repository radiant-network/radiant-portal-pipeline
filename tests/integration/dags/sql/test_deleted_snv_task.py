"""A part holding a deleted germline SNV task: the run purges its occurrences and its staging frequencies.

Driven the way import_part drives it: the real RadiantStarRocksPartitionSwapOperator with the occurrence copy and
delta insert (no task to load, so `task_ids` is `[-1]`), then the staging frequency insert. The deleted task is alone
in its part, or next to a live task imported earlier, which the copy step keeps. Ids 40xxx, removed after the test.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.starrocks.operator import RadiantStarRocksPartitionSwapOperator, SwapPartition

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_PART = 4001
_TENANT = "tenant1"
_DELETED_TASK, _DELETED_SEQ = 40001, 4000101
_LIVE_TASK, _LIVE_SEQ = 40002, 4000201
_SHARED_LOCUS, _DELETED_ONLY_LOCUS = 4000001, 4000002


class _SessionHook:
    """The subset of the DB-API hook the swap operator calls, on the test's pymysql session."""

    def __init__(self, session):
        self.session = session

    def run(self, sql, autocommit=True, parameters=None):
        with self.session.cursor() as cursor:
            cursor.execute(sql, parameters)

    def get_first(self, sql):
        with self.session.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()


def _render(path, mapping, **context):
    with open(os.path.join(_SQL_DIR, path)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping, **context})


def _fetch(cursor, sql, params):
    cursor.execute(sql, params)
    return cursor.fetchall()


@pytest.fixture(params=[False, True], ids=["alone", "with-live-task"])
def part_with_deleted_task(request, starrocks_session, setup_iceberg_namespace, radiant_mapping):
    """The part after an import, then the deletion of the germline task 40001: its occurrences and staging frequencies
    are in place, and its staging row is flagged. With a live task, 40002 shares one of its loci."""
    with_live_task = request.param
    staging = radiant_mapping["starrocks_staging_sequencing_experiment"]
    occurrence = radiant_mapping["starrocks_germline_snv_occurrence"]
    freq = radiant_mapping["starrocks_germline_snv_staging_variant_frequency"]

    def cleanup():
        with starrocks_session.cursor() as cursor:
            cursor.execute(f"DELETE FROM {staging} WHERE task_id IN (%s, %s)", (_DELETED_TASK, _LIVE_TASK))
            cursor.execute(f"DELETE FROM {occurrence} WHERE part = %(part)s", {"part": _PART})
            cursor.execute(f"DELETE FROM {freq} WHERE part = %(part)s", {"part": _PART})

    with starrocks_session.cursor() as cursor:
        for table in (
            "staging_sequencing_experiment",
            "germline_snv_occurrence",
            "germline_snv_staging_variant_frequency",
            "snv_tmp_variant",
            "exomiser",
            "germline_snv_comphet",
        ):
            cursor.execute(_render(f"radiant/init/{table}_create_table.sql", radiant_mapping))
    cleanup()

    tasks = [(_DELETED_TASK, _DELETED_SEQ, (_SHARED_LOCUS, _DELETED_ONLY_LOCUS))]
    if with_live_task:
        tasks.append((_LIVE_TASK, _LIVE_SEQ, (_SHARED_LOCUS,)))
    with starrocks_session.cursor() as cursor:
        for task_id, seq_id, loci in tasks:
            cursor.execute(
                f"INSERT INTO {staging} (case_id, seq_id, task_id, task_type, part, analysis_type, patient_id, "
                "experimental_strategy, affected_status, tenant_code) "
                "VALUES (%s, %s, %s, 'radiant_germline_annotation', %s, 'germline', %s, 'wxs', 'affected', %s)",
                (task_id, seq_id, task_id, _PART, f"p{task_id}", _TENANT),
            )
            cursor.executemany(
                f"INSERT INTO {occurrence} (part, seq_id, task_id, locus_id, gq, filter, ad_alt, zygosity, phased) "
                "VALUES (%s, %s, %s, %s, 50, 'PASS', 7, 'HET', false)",
                [(_PART, seq_id, task_id, locus_id) for locus_id in loci],
            )
        cursor.execute(
            _render("radiant/germline_snv_staging_variant_freq_insert.sql", radiant_mapping),
            {"part": _PART, "tenant_code": _TENANT},
        )
        cursor.execute(f"UPDATE {staging} SET deleted = true WHERE task_id = %(task_id)s", {"task_id": _DELETED_TASK})

    yield with_live_task
    cleanup()


def test_deleted_germline_task_loses_its_occurrences_and_frequencies(
    starrocks_session, radiant_mapping, part_with_deleted_task
):
    with_live_task = part_with_deleted_task
    occurrence = radiant_mapping["starrocks_germline_snv_occurrence"]
    freq = radiant_mapping["starrocks_germline_snv_staging_variant_frequency"]
    hook = _SessionHook(starrocks_session)

    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping['iceberg_germline_snv_occurrence']}")

    copy_sql = _render(
        "radiant/germline_snv_occurrence_copy_partition.sql", radiant_mapping, table=occurrence, partition=_PART
    )
    swap = RadiantStarRocksPartitionSwapOperator(
        task_id="insert_germline_snv_occurrence",
        table=occurrence,
        swap_partition=SwapPartition(partition=str(_PART), copy_partition_sql=copy_sql.rstrip().rstrip(";")),
        insert_partition_sql=_render(
            "radiant/germline_snv_occurrence_insert_partition_delta.sql", radiant_mapping, partition=_PART
        ),
        parameters={"task_ids": [-1], "deleted_task_ids": [_DELETED_TASK], "tenant_code": _TENANT},
    )
    swap.get_db_hook = lambda: hook
    swap.execute(context={})

    params = {"part": _PART, "tenant_code": _TENANT}
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render("radiant/germline_snv_staging_variant_freq_insert.sql", radiant_mapping), params)
        occurrences = _fetch(cursor, f"SELECT task_id, locus_id FROM {occurrence} WHERE part = %(part)s", params)
        freqs = _fetch(
            cursor,
            f"SELECT locus_id, pc_wxs, pn_wxs FROM {freq} WHERE tenant_code = %(tenant_code)s AND part = %(part)s",
            params,
        )

    if with_live_task:
        # The live task keeps its occurrence and is the only carrier left, out of one patient.
        assert occurrences == ((_LIVE_TASK, _SHARED_LOCUS),)
        assert freqs == ((_SHARED_LOCUS, 1, 1),)
    else:
        # The insert writes no row: the named partition is emptied, not left with its old rows.
        assert occurrences == ()
        assert freqs == ()
