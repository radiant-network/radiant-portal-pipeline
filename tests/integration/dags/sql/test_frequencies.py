import csv
import os

import jinja2
import pandas as pd
import pytest

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")


def _reset_table(starrocks_session, table_name, mapping):
    with open(os.path.join(_SQL_DIR, f"radiant/init/{table_name}_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"mapping": mapping})

    table_name = mapping.get(f"starrocks_{table_name}")

    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        cursor.execute(f"TRUNCATE TABLE {table_name};")


def load_tsv(starrocks_session, table_name, tsv_path):
    rows = pd.read_csv(tsv_path, delimiter="\t", quoting=csv.QUOTE_NONE)
    rows = rows.replace(to_replace=float("nan"), value=None).to_dict(orient="records")
    columns = list(rows[0].keys())
    insert_sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({", ".join(["%s"] * len(columns))})
    """
    values = []
    for row in rows:
        value_tuple = [row[col] for col in columns]
        values.append(value_tuple)

    with starrocks_session.cursor() as cursor:
        cursor.executemany(insert_sql, values)


def test_staging_variant_frequencies_calculation(starrocks_session, resources_dir, radiant_mapping):
    """
    Test the frequencies calculation for variants.
    """

    for table_name in [
        "germline_snv_occurrence",
        "staging_sequencing_experiment",
        "germline_snv_staging_variant_frequency",
    ]:
        _reset_table(starrocks_session, table_name, radiant_mapping)

    # Insert some test data into the occurrence table
    occurrence_table_name = radiant_mapping.get("starrocks_germline_snv_occurrence")
    seq_exp_table = radiant_mapping.get("starrocks_staging_sequencing_experiment")

    load_tsv(starrocks_session, occurrence_table_name, resources_dir / "radiant/occurrence.tsv")
    load_tsv(starrocks_session, seq_exp_table, resources_dir / "radiant/staging_sequencing_experiment.tsv")

    with open(os.path.join(_SQL_DIR, "radiant/germline_snv_staging_variant_freq_insert.sql")) as f_in:
        variant_freq_insert = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})

    _select_sql = "SELECT * FROM {{ mapping.starrocks_germline_snv_staging_variant_frequency }}"
    _select_sql = jinja2.Template(_select_sql).render({"mapping": radiant_mapping})

    _params = {"part": 0, "tenant_code": "tenant1"}

    # Insert the data into the occurrence table
    with starrocks_session.cursor() as cursor:
        cursor.execute(variant_freq_insert, _params)
        cursor.execute(_select_sql)

        results = cursor.fetchall()
        # Values vetted with the content of test resource staging_sequencing_experiment.tsv
        assert results == (("tenant1", 0, -8935141392267608062, 5, 10, 3, 7, 2, 3, 0, 0, 0, 0, 0, 0),)


# --- Somatic tumor-only / tumor-normal frequencies (SJRA-1751) ----------------------------------
#
# One partition holding, at wgs: a tumor-normal-only patient (1), a tumor-only-only patient (2), and
# patient 3 whose single tumor sample (seq 5, aliquot A3-T) carries BOTH a tumor-normal task and a
# tumor-only task. Plus patients whose task belongs to a cohort but who carry no qualifying locus in
# this partition (6, 8, 10 for tumor-normal; 7 for tumor-only), so the denominators are all
# different — 4 / 2 / 3 / 1 — and no bucket or numerator/denominator swap can pass.

_SOMATIC = "radiant_somatic_annotation"
_GERMLINE = "radiant_germline_annotation"

_SEQ_COLUMNS = (
    "case_id",
    "seq_id",
    "task_id",
    "task_type",
    "part",
    "analysis_type",
    "aliquot",
    "patient_id",
    "experimental_strategy",
    "histology_type",
    "tenant_code",
)

# (case_id, seq_id, task_id, task_type, part, analysis_type, aliquot, patient_id, strategy,
#  histology_type, tenant_code)
_SEQ_ROWS = [
    # tumor-normal wgs — patient 1
    (1, 1, 101, _SOMATIC, 0, "somatic", "A1-T", "1", "wgs", "tumoral", "tenant1"),
    (1, 2, 101, _SOMATIC, 0, "somatic", "A1-N", "1", "wgs", "normal", "tenant1"),
    # tumor-only wgs — patient 2
    (2, 3, 102, _SOMATIC, 0, "somatic", "A2-T", "2", "wgs", "tumoral", "tenant1"),
    # patient 3: ONE tumor sample (seq 5 / aliquot A3-T) analysed both ways. The tumor-only task 104
    # must not leak into the tumor-normal cohort, nor the tumor-normal task 103 into tumor-only.
    (3, 5, 103, _SOMATIC, 0, "somatic", "A3-T", "3", "wgs", "tumoral", "tenant1"),
    (3, 6, 103, _SOMATIC, 0, "somatic", "A3-N", "3", "wgs", "normal", "tenant1"),
    (3, 5, 104, _SOMATIC, 0, "somatic", "A3-T", "3", "wgs", "tumoral", "tenant1"),
    # tumor-normal wxs — patient 4 ; tumor-only wxs — patient 5
    (4, 7, 105, _SOMATIC, 0, "somatic", "A4-T", "4", "wxs", "tumoral", "tenant1"),
    (4, 8, 105, _SOMATIC, 0, "somatic", "A4-N", "4", "wxs", "normal", "tenant1"),
    (5, 9, 106, _SOMATIC, 0, "somatic", "A5-T", "5", "wxs", "tumoral", "tenant1"),
    # cohort-only patients: a task in this partition, but no qualifying locus
    (6, 11, 107, _SOMATIC, 0, "somatic", "A6-T", "6", "wgs", "tumoral", "tenant1"),
    (6, 12, 107, _SOMATIC, 0, "somatic", "A6-N", "6", "wgs", "normal", "tenant1"),
    (7, 13, 108, _SOMATIC, 0, "somatic", "A7-T", "7", "wgs", "tumoral", "tenant1"),
    (8, 15, 109, _SOMATIC, 0, "somatic", "A8-T", "8", "wgs", "tumoral", "tenant1"),
    (8, 16, 109, _SOMATIC, 0, "somatic", "A8-N", "8", "wgs", "normal", "tenant1"),
    (10, 19, 111, _SOMATIC, 0, "somatic", "A10-T", "10", "wxs", "tumoral", "tenant1"),
    (10, 20, 111, _SOMATIC, 0, "somatic", "A10-N", "10", "wxs", "normal", "tenant1"),
    # sentinels that must be excluded: another tenant, a germline task, another part
    (9, 17, 110, _SOMATIC, 0, "somatic", "A9-T", "9", "wgs", "tumoral", "tenant2"),
    (11, 21, 113, _GERMLINE, 0, "germline", "A11", "11", "wgs", "normal", "tenant1"),
    (12, 23, 114, _SOMATIC, 1, "somatic", "A12-T", "12", "wgs", "tumoral", "tenant1"),
]

# A malformed task: two tumoral aliquots and no normal. The VCF loader raises for these, so they
# never produce occurrences, but they are still in staging_sequencing_experiment. They belong to
# neither cohort — `NOT is_tumor_only` would have swept them into the tumor-normal denominator.
_MALFORMED_SEQ_ROWS = [
    (13, 25, 115, _SOMATIC, 0, "somatic", "A13-T1", "13", "wgs", "tumoral", "tenant1"),
    (13, 26, 115, _SOMATIC, 0, "somatic", "A13-T2", "13", "wgs", "tumoral", "tenant1"),
]

_OCC_COLUMNS = ("part", "task_id", "tumor_seq_id", "locus_id", "filter", "tumor_ad_alt")
_OCC_ROWS = [
    (0, 101, 1, 1001, "PASS", 5),  # tumor-normal wgs carrier, patient 1
    (0, 104, 5, 1001, "PASS", 5),  # tumor-only carrier on the SHARED tumor sample, patient 3
    (0, 102, 3, 1001, "PASS", 5),  # tumor-only wgs carrier, patient 2
    (0, 103, 5, 2002, "PASS", 7),  # tumor-normal carrier on the SHARED tumor sample, patient 3
    (0, 105, 7, 2002, "PASS", 4),  # tumor-normal wxs carrier, patient 4
    (0, 106, 9, 2002, "PASS", 4),  # tumor-only wxs carrier, patient 5
    (0, 108, 13, 3003, "weak_evidence", 9),  # dropped: filter <> 'PASS'
    (0, 101, 1, 3003, "PASS", 2),  # dropped: tumor_ad_alt not > 2
    (1, 101, 1, 4004, "PASS", 9),  # dropped: other part
    (0, 110, 17, 1001, "PASS", 9),  # dropped: tenant2 task is absent from somatic_tasks
    (1, 114, 23, 1001, "PASS", 6),  # part 1 — used by the rollup test only
]

_SOMATIC_TABLES = (
    "staging_sequencing_experiment",
    "somatic_snv_occurrence",
    "somatic_snv_staging_variant_frequency",
)


def _seed(cursor, table, columns, rows):
    quoted = ", ".join(f"`{column}`" for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    cursor.executemany(f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})", rows)


def _seed_somatic_cohort(starrocks_session, radiant_mapping, extra_seq_rows=()):
    for table_name in _SOMATIC_TABLES:
        _reset_table(starrocks_session, table_name, radiant_mapping)

    with starrocks_session.cursor() as cursor:
        _seed(
            cursor,
            radiant_mapping["starrocks_staging_sequencing_experiment"],
            _SEQ_COLUMNS,
            [*_SEQ_ROWS, *extra_seq_rows],
        )
        _seed(cursor, radiant_mapping["starrocks_somatic_snv_occurrence"], _OCC_COLUMNS, _OCC_ROWS)


def _run_staging_freq_insert(starrocks_session, radiant_mapping, part):
    with open(os.path.join(_SQL_DIR, "radiant/somatic_snv_staging_variant_freq_insert.sql")) as f_in:
        insert_sql = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})

    with starrocks_session.cursor() as cursor:
        cursor.execute(insert_sql, {"part": part, "tenant_code": "tenant1"})


def _fetch_staging_freqs(starrocks_session, radiant_mapping):
    table = radiant_mapping["starrocks_somatic_snv_staging_variant_frequency"]
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table} ORDER BY part, locus_id")
        return cursor.fetchall()


def test_somatic_staging_variant_frequencies_mixed_cohort(starrocks_session, radiant_mapping):
    """Tumor-only and tumor-normal frequencies over a partition where one tumor sample carries both.

    Regression for the pre-SJRA-1751 query, which was case-grained and joined carriers on
    `s.seq_id = o.tumor_seq_id`. On this fixture it reported pc_tn_wgs = 3 at locus 1001 (patients 2
    and 3 leaking in from their tumor-only tasks) and pc_tn_wxs = 2 at locus 2002 (tumor-only-only
    patient 5 leaking in), instead of 1 and 1. Its denominators happened to agree, so only the
    numerators were wrong — which is exactly why it went unnoticed.
    """
    _seed_somatic_cohort(starrocks_session, radiant_mapping)
    _run_staging_freq_insert(starrocks_session, radiant_mapping, part=0)

    rows = _fetch_staging_freqs(starrocks_session, radiant_mapping)

    # Locus 3003 (one row fails `filter`, the other `tumor_ad_alt`) and 4004 (other part) are absent
    # rather than present with zero counts.
    assert [row[:3] for row in rows] == [("tenant1", 0, 1001), ("tenant1", 0, 2002)]

    #        pc_tn_wgs pn pf     pc_tn_wxs pn pf      pc_to_wgs pn pf      pc_to_wxs pn pf
    expected = {
        1001: (1, 4, 1 / 4, 0, 2, 0.0, 2, 3, 2 / 3, 0, 1, 0.0),
        2002: (1, 4, 1 / 4, 1, 2, 1 / 2, 0, 3, 0.0, 1, 1, 1.0),
    }
    for row in rows:
        actual = tuple(float(value) for value in row[3:])
        assert actual == pytest.approx(expected[row[2]]), f"locus {row[2]}"

    # Patient 3 carries a tumor-only AND a tumor-normal task on one sample, so they belong to both
    # cohorts: cnt_tn_wgs = {1, 3, 6, 8} = 4 and cnt_to_wgs = {2, 3, 7} = 3.
    assert {row[4] for row in rows} == {4}
    assert {row[10] for row in rows} == {3}


def test_somatic_staging_variant_frequencies_exclude_malformed_task(starrocks_session, radiant_mapping):
    """A somatic task with two tumoral aliquots and no normal belongs to neither cohort."""
    _seed_somatic_cohort(starrocks_session, radiant_mapping, extra_seq_rows=_MALFORMED_SEQ_ROWS)
    _run_staging_freq_insert(starrocks_session, radiant_mapping, part=0)

    rows = _fetch_staging_freqs(starrocks_session, radiant_mapping)

    # Patient 13 enters neither denominator, so both are unchanged from the mixed-cohort test.
    assert {row[4] for row in rows} == {4}
    assert {row[10] for row in rows} == {3}


def test_somatic_variant_frequencies_rollup_across_parts(starrocks_session, radiant_mapping):
    """The level-2 rollup sums pc_* per locus and pn_* across parts, for tumor-only as for tumor-normal.

    Part 1 adds one tumor-only wgs patient (12, task 114) carrying locus 1001, so pn_to_wgs becomes
    3 + 1 = 4 and pc_to_wgs at that locus becomes 2 + 1 = 3.

    Note pn_* is *summed* over parts, so a patient with tasks in two partitions is counted twice in
    the denominator. That is pre-existing behaviour, identical for tumor-normal, and not changed here.
    """
    _seed_somatic_cohort(starrocks_session, radiant_mapping)
    _reset_table(starrocks_session, "somatic_snv_variant_frequency", radiant_mapping)

    _run_staging_freq_insert(starrocks_session, radiant_mapping, part=0)
    _run_staging_freq_insert(starrocks_session, radiant_mapping, part=1)

    with open(os.path.join(_SQL_DIR, "radiant/somatic_snv_variant_frequency_insert.sql")) as f_in:
        rollup_sql = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})

    table = radiant_mapping["starrocks_somatic_snv_variant_frequency"]
    with starrocks_session.cursor() as cursor:
        cursor.execute(rollup_sql, {"tenant_code": "tenant1"})
        cursor.execute(f"SELECT * FROM {table} ORDER BY locus_id")
        rows = cursor.fetchall()

    #        pc_tn_wgs pn pf      pc_tn_wxs pn pf      pc_to_wgs pn pf      pc_to_wxs pn pf
    expected = {
        1001: (1, 4, 0.25, 0, 2, 0.0, 3, 4, 0.75, 0, 1, 0.0),
        2002: (1, 4, 0.25, 1, 2, 0.5, 0, 4, 0.0, 1, 1, 1.0),
    }
    assert {row[0] for row in rows} == set(expected)
    for row in rows:
        actual = tuple(float(value) for value in row[1:])
        assert actual == pytest.approx(expected[row[0]]), f"locus {row[0]}"
