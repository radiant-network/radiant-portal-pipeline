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
        # Values vetted with the content of test resource staging_sequencing_experiment.tsv. The six trailing
        # hom_* are 0: the HOM carriers of the fixture sit at the other locus, dropped by `gq` (RAD-22).
        assert results == (
            ("tenant1", 0, -8935141392267608062, 5, 10, 3, 7, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        )


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

_OCC_COLUMNS = ("part", "task_id", "tumor_seq_id", "locus_id", "filter", "tumor_ad_alt", "tumor_zygosity")
_OCC_ROWS = [
    (0, 101, 1, 1001, "PASS", 5, "HOM"),  # tumor-normal wgs carrier, patient 1
    (0, 104, 5, 1001, "PASS", 5, "HEM"),  # tumor-only carrier on the SHARED tumor sample, patient 3
    (0, 102, 3, 1001, "PASS", 5, "HET"),  # tumor-only wgs carrier, patient 2
    (0, 103, 5, 2002, "PASS", 7, "HET"),  # tumor-normal carrier on the SHARED tumor sample, patient 3
    (0, 105, 7, 2002, "PASS", 4, "HOM"),  # tumor-normal wxs carrier, patient 4
    (0, 106, 9, 2002, "PASS", 4, "HEM"),  # tumor-only wxs carrier, patient 5
    (0, 108, 13, 3003, "weak_evidence", 9, "HOM"),  # dropped: filter <> 'PASS'
    (0, 101, 1, 3003, "PASS", 2, "HOM"),  # dropped: tumor_ad_alt not > 2
    (1, 101, 1, 4004, "PASS", 9, "HOM"),  # dropped: other part
    (0, 110, 17, 1001, "PASS", 9, "HOM"),  # dropped: tenant2 task is absent from somatic_tasks
    (1, 114, 23, 1001, "PASS", 6, "HOM"),  # part 1 — used by the rollup test only
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

    # The trailing hom_tn_wgs, hom_tn_wxs, hom_to_wgs, hom_to_wxs (RAD-22) count the HOM / HEM tumor calls: at
    # 1001 patient 1 (tumor-normal, HOM) and patient 3 (tumor-only, HEM) but not patient 2 (HET); at 2002
    # patients 4 (tumor-normal wxs, HOM) and 5 (tumor-only wxs, HEM). The dropped rows are all HOM.
    #        pc_tn_wgs pn pf     pc_tn_wxs pn pf      pc_to_wgs pn pf      pc_to_wxs pn pf  hom_* x4
    expected = {
        1001: (1, 4, 1 / 4, 0, 2, 0.0, 2, 3, 2 / 3, 0, 1, 0.0, 1, 0, 1, 0),
        2002: (1, 4, 1 / 4, 1, 2, 1 / 2, 0, 3, 0.0, 1, 1, 1.0, 0, 1, 0, 1),
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

    # Patient 12 is HOM at 1001, so hom_to_wgs there becomes 1 + 1 = 2 (RAD-22).
    #        pc_tn_wgs pn pf      pc_tn_wxs pn pf      pc_to_wgs pn pf      pc_to_wxs pn pf  hom_* x4
    expected = {
        1001: (1, 4, 0.25, 0, 2, 0.0, 3, 4, 0.75, 0, 1, 0.0, 1, 0, 2, 0),
        2002: (1, 4, 0.25, 1, 2, 0.5, 0, 4, 0.0, 1, 1, 1.0, 0, 1, 0, 1),
    }
    assert {row[0] for row in rows} == set(expected)
    for row in rows:
        actual = tuple(float(value) for value in row[1:])
        assert actual == pytest.approx(expected[row[0]]), f"locus {row[0]}"


# --- Germline homozygous counts (RAD-22) ---------------------------------------------------------
#
# Part 0 holds two loci. At 1001: wgs affected patients p1 (HOM), p2 (HET) and p5, who has two samples, HOM in
# one and HET in the other; wgs not-affected p3 (HEM) and p4 (HET); wxs affected p7 (HEM) and p9 (HOM, dropped by
# `gq`); wxs not-affected p8 (HET). At 2002: only p2 (HET) qualifies, the HOM rows of p4 and p8 fail `filter` and
# `ad_alt`. Part 1 adds one wgs affected patient, p10, HOM at 1001, for the rollup test.

_GERMLINE_SEQ_COLUMNS = (
    "case_id",
    "seq_id",
    "task_id",
    "task_type",
    "part",
    "analysis_type",
    "patient_id",
    "experimental_strategy",
    "affected_status",
    "tenant_code",
)

# (case_id, seq_id, task_id, task_type, part, analysis_type, patient_id, strategy, affected_status, tenant_code)
_GERMLINE_SEQ_ROWS = [
    (1, 1, 201, _GERMLINE, 0, "germline", "p1", "wgs", "affected", "tenant1"),
    (2, 2, 202, _GERMLINE, 0, "germline", "p2", "wgs", "affected", "tenant1"),
    (3, 3, 203, _GERMLINE, 0, "germline", "p3", "wgs", "non_affected", "tenant1"),
    (4, 4, 204, _GERMLINE, 0, "germline", "p4", "wgs", "non_affected", "tenant1"),
    (5, 5, 205, _GERMLINE, 0, "germline", "p5", "wgs", "affected", "tenant1"),
    (5, 6, 206, _GERMLINE, 0, "germline", "p5", "wgs", "affected", "tenant1"),
    (7, 7, 207, _GERMLINE, 0, "germline", "p7", "wxs", "affected", "tenant1"),
    (8, 8, 208, _GERMLINE, 0, "germline", "p8", "wxs", "non_affected", "tenant1"),
    (9, 9, 209, _GERMLINE, 0, "germline", "p9", "wxs", "affected", "tenant1"),
    (10, 10, 210, _GERMLINE, 1, "germline", "p10", "wgs", "affected", "tenant1"),
]

_GERMLINE_OCC_COLUMNS = ("part", "seq_id", "task_id", "locus_id", "gq", "filter", "ad_alt", "zygosity", "phased")
_GERMLINE_OCC_ROWS = [
    (0, 1, 201, 1001, 50, "PASS", 7, "HOM", False),
    (0, 2, 202, 1001, 50, "PASS", 7, "HET", False),
    (0, 3, 203, 1001, 50, "PASS", 7, "HEM", False),
    (0, 4, 204, 1001, 50, "PASS", 7, "HET", False),
    (0, 5, 205, 1001, 50, "PASS", 7, "HOM", False),  # p5, first sample
    (0, 6, 206, 1001, 50, "PASS", 7, "HET", False),  # p5, second sample: still one hom patient
    (0, 7, 207, 1001, 50, "PASS", 7, "HEM", False),
    (0, 8, 208, 1001, 50, "PASS", 7, "HET", False),
    (0, 9, 209, 1001, 10, "PASS", 7, "HOM", False),  # dropped: gq < 20
    (0, 2, 202, 2002, 50, "PASS", 7, "HET", False),
    (0, 4, 204, 2002, 50, "LowQual", 7, "HOM", False),  # dropped: filter <> 'PASS'
    (0, 8, 208, 2002, 50, "PASS", 3, "HOM", False),  # dropped: ad_alt not > 3
    (1, 10, 210, 1001, 50, "PASS", 7, "HOM", False),  # part 1, rollup test only
]


def _seed_germline_cohort(starrocks_session, radiant_mapping):
    for table_name in (
        "staging_sequencing_experiment",
        "germline_snv_occurrence",
        "germline_snv_staging_variant_frequency",
        "germline_snv_variant_frequency",
    ):
        _reset_table(starrocks_session, table_name, radiant_mapping)

    with starrocks_session.cursor() as cursor:
        _seed(
            cursor,
            radiant_mapping["starrocks_staging_sequencing_experiment"],
            _GERMLINE_SEQ_COLUMNS,
            _GERMLINE_SEQ_ROWS,
        )
        _seed(
            cursor,
            radiant_mapping["starrocks_germline_snv_occurrence"],
            _GERMLINE_OCC_COLUMNS,
            _GERMLINE_OCC_ROWS,
        )


def _run_sql(starrocks_session, radiant_mapping, sql_file, params):
    with open(os.path.join(_SQL_DIR, "radiant", sql_file)) as f_in:
        sql = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql, params)


def _fetch_by_name(starrocks_session, table, columns, order_by):
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"SELECT {', '.join(columns)} FROM {table} ORDER BY {order_by}")
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


_GERMLINE_HOM_COLUMNS = (
    "hom_wgs",
    "hom_wgs_affected",
    "hom_wgs_not_affected",
    "hom_wxs",
    "hom_wxs_affected",
    "hom_wxs_not_affected",
)


def test_germline_staging_variant_frequencies_hom(starrocks_session, radiant_mapping):
    """hom counts the distinct qualifying carriers called HOM or HEM, per cohort, next to pc."""
    _seed_germline_cohort(starrocks_session, radiant_mapping)
    _run_sql(
        starrocks_session,
        radiant_mapping,
        "germline_snv_staging_variant_freq_insert.sql",
        {"part": 0, "tenant_code": "tenant1"},
    )

    rows = _fetch_by_name(
        starrocks_session,
        radiant_mapping["starrocks_germline_snv_staging_variant_frequency"],
        (
            "tenant_code",
            "part",
            "locus_id",
            "pc_wgs",
            "pc_wgs_affected",
            "pc_wgs_not_affected",
            "pc_wxs",
            "pc_wxs_affected",
            "pc_wxs_not_affected",
            "pn_wgs",
            "pn_wxs",
            *_GERMLINE_HOM_COLUMNS,
        ),
        "locus_id",
    )

    assert [(row["tenant_code"], row["part"], row["locus_id"]) for row in rows] == [
        ("tenant1", 0, 1001),
        ("tenant1", 0, 2002),
    ]
    by_locus = {row["locus_id"]: row for row in rows}

    # pc is unchanged by RAD-22: p5 counts once although two of their samples carry the locus.
    locus_1001 = by_locus[1001]
    assert (
        locus_1001["pc_wgs"],
        locus_1001["pc_wgs_affected"],
        locus_1001["pc_wgs_not_affected"],
        locus_1001["pc_wxs"],
        locus_1001["pc_wxs_affected"],
        locus_1001["pc_wxs_not_affected"],
    ) == (5, 3, 2, 2, 1, 1)
    assert (locus_1001["pn_wgs"], locus_1001["pn_wxs"]) == (5, 3)

    # wgs: p1 (HOM), p3 (HEM), p5 (HOM in one sample, HET in the other) -> 3, of which p1, p5 affected and p3
    # not. wxs: p7 (HEM) only, p9's HOM call fails `gq`.
    assert tuple(locus_1001[column] for column in _GERMLINE_HOM_COLUMNS) == (3, 2, 1, 1, 1, 0)

    # The HOM calls of p4 and p8 at 2002 fail `filter` and `ad_alt`, so they count in neither pc nor hom.
    locus_2002 = by_locus[2002]
    assert (locus_2002["pc_wgs"], locus_2002["pc_wxs"]) == (1, 0)
    assert tuple(locus_2002[column] for column in _GERMLINE_HOM_COLUMNS) == (0, 0, 0, 0, 0, 0)


def test_germline_variant_frequencies_hom_rollup_across_parts(starrocks_session, radiant_mapping):
    """The tenant rollup sums hom_* across parts, as it does for pc_*."""
    _seed_germline_cohort(starrocks_session, radiant_mapping)
    for part in (0, 1):
        _run_sql(
            starrocks_session,
            radiant_mapping,
            "germline_snv_staging_variant_freq_insert.sql",
            {"part": part, "tenant_code": "tenant1"},
        )
    _run_sql(
        starrocks_session, radiant_mapping, "germline_snv_variant_frequency_insert.sql", {"tenant_code": "tenant1"}
    )

    rows = _fetch_by_name(
        starrocks_session,
        radiant_mapping["starrocks_germline_snv_variant_frequency"],
        ("locus_id", "pc_wgs", "pn_wgs", "pc_wgs_affected", "pn_wgs_affected", *_GERMLINE_HOM_COLUMNS),
        "locus_id",
    )
    by_locus = {row["locus_id"]: row for row in rows}

    # p10 (part 1, wgs affected, HOM at 1001) adds one to pc, pn and hom of wgs and wgs affected.
    locus_1001 = by_locus[1001]
    assert (
        locus_1001["pc_wgs"],
        locus_1001["pn_wgs"],
        locus_1001["pc_wgs_affected"],
        locus_1001["pn_wgs_affected"],
    ) == (6, 6, 4, 4)
    assert tuple(locus_1001[column] for column in _GERMLINE_HOM_COLUMNS) == (4, 3, 1, 1, 1, 0)
    assert tuple(by_locus[2002][column] for column in _GERMLINE_HOM_COLUMNS) == (0, 0, 0, 0, 0, 0)
