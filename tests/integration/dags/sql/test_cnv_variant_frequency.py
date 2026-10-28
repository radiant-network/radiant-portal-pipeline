"""RAD-120 -- in-cohort CNV frequencies: 80% reciprocal overlap, per distinct interval (`cnv_id`).

The matching is a binned self-join: a wrong bin, class or threshold fails silently, with plausible counts. So
every boundary is pinned on values: a match at exactly 80% and a miss one base under it, GAIN against GAINLOH,
LOSS against GAIN, intervals of two parts, a non-PASS carrier, and a patient with two matching segments.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_GERMLINE = "alignment_germline_variant_calling"
_TUMOR_ONLY = "tumor_only_variant_calling"

# Intervals, all on chromosome 1. A, B and C match each other; D starts one base after C, so its overlap with A
# is 799 999, one base under 80% of A's 1 000 000, but it still matches B and C. E (GAIN) and F (GAINLOH) share
# coordinates and match each other; G is a GAIN with A's coordinates and does not match A (LOSS). H and H2 are
# two segments of one patient that match each other. I has non-PASS carriers only. Z has no length.
_A, _B, _C, _D, _E, _F, _G, _H, _H2, _I, _Z = 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111

# cnv_id -> (start, end, type)
_INTERVALS = {
    _A: (1_000_000, 2_000_000, "LOSS"),
    _B: (1_100_000, 2_000_000, "LOSS"),
    _C: (1_200_000, 2_000_000, "LOSS"),
    _D: (1_200_001, 2_000_000, "LOSS"),
    _E: (5_000_000, 6_000_000, "GAIN"),
    _F: (5_000_000, 6_000_000, "GAINLOH"),
    _G: (1_000_000, 2_000_000, "GAIN"),
    _H: (10_000_000, 10_100_000, "LOSS"),
    _H2: (10_000_000, 10_095_000, "LOSS"),
    _I: (20_000_000, 20_500_000, "LOSS"),
    _Z: (30_000_000, 30_000_000, "LOSS"),
}

_OCC_COLUMNS = ("part", "seq_id", "task_id", "cnv_id", "chromosome", "start", "end", "type", "filter")


def _occurrence(part, seq_id, task_id, cnv_id, filter_="PASS"):
    start, end, type_ = _INTERVALS[cnv_id]
    return part, seq_id, task_id, cnv_id, "1", start, end, type_, filter_


def _reset_table(starrocks_session, table_name, mapping):
    with open(os.path.join(_SQL_DIR, f"radiant/init/{table_name}_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"mapping": mapping})
    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        cursor.execute(f"TRUNCATE TABLE {mapping[f'starrocks_{table_name}']};")


def _seed(starrocks_session, table, columns, rows):
    quoted = ", ".join(f"`{column}`" for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    with starrocks_session.cursor() as cursor:
        cursor.executemany(f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})", rows)


def _run_insert(starrocks_session, radiant_mapping, sql_file):
    with open(os.path.join(_SQL_DIR, "radiant", sql_file)) as f_in:
        sql = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql, {"tenant_code": "tenant1"})


def _fetch(starrocks_session, table, columns):
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"SELECT cnv_id, {', '.join(columns)} FROM {table} ORDER BY cnv_id")
        return {row[0]: dict(zip(columns, row[1:], strict=True)) for row in cursor.fetchall()}


# --- Germline ------------------------------------------------------------------------------------
#
# wgs: p1 and p2 affected, p3 and p4 not affected, p6 affected with a non-PASS CNV only. wxs: p5 affected.
# p7 has a staging row but no CNV, p8 belongs to tenant2: neither is in a denominator.

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

_GERMLINE_SEQ_ROWS = [
    (1, 1, 201, _GERMLINE, 0, "germline", "p1", "wgs", "affected", "tenant1"),
    (2, 2, 202, _GERMLINE, 1, "germline", "p2", "wgs", "affected", "tenant1"),
    (3, 3, 203, _GERMLINE, 0, "germline", "p3", "wgs", "non_affected", "tenant1"),
    (4, 4, 204, _GERMLINE, 0, "germline", "p4", "wgs", "non_affected", "tenant1"),
    (5, 5, 205, _GERMLINE, 0, "germline", "p5", "wxs", "affected", "tenant1"),
    (6, 6, 206, _GERMLINE, 0, "germline", "p6", "wgs", "affected", "tenant1"),
    (7, 7, 207, _GERMLINE, 0, "germline", "p7", "wgs", "affected", "tenant1"),
    (8, 8, 208, _GERMLINE, 0, "germline", "p8", "wgs", "affected", "tenant2"),
]

_GERMLINE_OCC_ROWS = [
    _occurrence(0, 1, 201, _A),
    _occurrence(1, 2, 202, _B),  # other part
    _occurrence(0, 3, 203, _C),
    _occurrence(0, 5, 205, _D),
    _occurrence(0, 1, 201, _E),
    _occurrence(1, 2, 202, _F),
    _occurrence(0, 4, 204, _G),
    _occurrence(0, 4, 204, _H),
    _occurrence(0, 4, 204, _H2),
    _occurrence(0, 6, 206, _I, filter_="LowQual"),
    _occurrence(0, 6, 206, _B, filter_="LowQual"),  # p6 matches A, B, C, D but never counts
    _occurrence(0, 1, 201, _Z),
    _occurrence(0, 8, 208, _A),  # tenant2 patient, excluded by the staging join
]

_GERMLINE_FREQ_COLUMNS = tuple(
    f"{measure}_{strategy}{cohort}"
    for strategy in ("wgs", "wxs")
    for cohort in ("", "_affected", "_not_affected")
    for measure in ("pc", "pn", "pf")
)


@pytest.fixture
def germline_frequencies(starrocks_session, radiant_mapping):
    for table_name in ("staging_sequencing_experiment", "germline_cnv_occurrence", "germline_cnv_variant_frequency"):
        _reset_table(starrocks_session, table_name, radiant_mapping)
    _seed(
        starrocks_session,
        radiant_mapping["starrocks_staging_sequencing_experiment"],
        _GERMLINE_SEQ_COLUMNS,
        _GERMLINE_SEQ_ROWS,
    )
    _seed(starrocks_session, radiant_mapping["starrocks_germline_cnv_occurrence"], _OCC_COLUMNS, _GERMLINE_OCC_ROWS)
    _run_insert(starrocks_session, radiant_mapping, "germline_cnv_variant_frequency_insert.sql")
    return _fetch(
        starrocks_session, radiant_mapping["starrocks_germline_cnv_variant_frequency"], _GERMLINE_FREQ_COLUMNS
    )


def test_germline_cnv_frequency_every_interval_has_a_row(germline_frequencies):
    """Non-PASS-only (I) and zero-length (Z) intervals included, with zero counts."""
    assert sorted(germline_frequencies) == sorted(_INTERVALS)
    for cnv_id in (_I, _Z):
        assert germline_frequencies[cnv_id]["pc_wgs"] == 0
        assert germline_frequencies[cnv_id]["pc_wxs"] == 0


def test_germline_cnv_frequency_denominators(germline_frequencies):
    """p6 (non-PASS only) is in pn; p7 (no CNV) and p8 (tenant2) are not. An empty cohort gives a NULL pf."""
    row = germline_frequencies[_A]
    assert (row["pn_wgs"], row["pn_wgs_affected"], row["pn_wgs_not_affected"]) == (5, 3, 2)
    assert (row["pn_wxs"], row["pn_wxs_affected"], row["pn_wxs_not_affected"]) == (1, 1, 0)
    assert row["pf_wxs_not_affected"] is None
    assert {r["pn_wgs"] for r in germline_frequencies.values()} == {5}


@pytest.mark.parametrize(
    "cnv_id, pc_wgs, pc_wgs_affected, pc_wgs_not_affected, pc_wxs",
    [
        # A: B (other part) and C (exactly 80% of A) match, D (one base under) does not.
        (_A, 3, 2, 1, 0),
        # B and C: A, B, C and D (p5, wxs) all match.
        (_B, 3, 2, 1, 1),
        (_C, 3, 2, 1, 1),
        # D: B, C and itself, not A.
        (_D, 2, 1, 1, 1),
        # GAIN and GAINLOH are one class.
        (_E, 2, 2, 0, 0),
        (_F, 2, 2, 0, 0),
        # LOSS and GAIN are not: G matches only itself.
        (_G, 1, 0, 1, 0),
        # p4 carries both H and H2, counted once.
        (_H, 1, 0, 1, 0),
        (_H2, 1, 0, 1, 0),
    ],
)
def test_germline_cnv_frequency_counts(
    germline_frequencies, cnv_id, pc_wgs, pc_wgs_affected, pc_wgs_not_affected, pc_wxs
):
    row = germline_frequencies[cnv_id]
    assert (row["pc_wgs"], row["pc_wgs_affected"], row["pc_wgs_not_affected"], row["pc_wxs"]) == (
        pc_wgs,
        pc_wgs_affected,
        pc_wgs_not_affected,
        pc_wxs,
    )
    assert row["pf_wgs"] == pytest.approx(pc_wgs / 5)
    assert row["pf_wxs"] == pytest.approx(pc_wxs / 1)


# --- Somatic -------------------------------------------------------------------------------------
#
# Tumor-only wgs: p1 (A), p2 (B), p3 (D, non-PASS). Tumor-only wxs: p5 (E), p6 (F). p4 has a tumor-normal task
# carrying C: C gets a row, but p4 belongs to no cohort.

_SOMATIC_SEQ_COLUMNS = (
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

_SOMATIC_SEQ_ROWS = [
    (11, 11, 301, _TUMOR_ONLY, 0, "somatic", "A1-T", "p1", "wgs", "tumoral", "tenant1"),
    (12, 12, 302, _TUMOR_ONLY, 1, "somatic", "A2-T", "p2", "wgs", "tumoral", "tenant1"),
    (13, 13, 303, _TUMOR_ONLY, 0, "somatic", "A3-T", "p3", "wgs", "tumoral", "tenant1"),
    (14, 14, 304, _TUMOR_ONLY, 0, "somatic", "A4-T", "p4", "wgs", "tumoral", "tenant1"),
    (14, 15, 304, _TUMOR_ONLY, 0, "somatic", "A4-N", "p4", "wgs", "normal", "tenant1"),
    (16, 16, 305, _TUMOR_ONLY, 0, "somatic", "A5-T", "p5", "wxs", "tumoral", "tenant1"),
    (17, 17, 306, _TUMOR_ONLY, 0, "somatic", "A6-T", "p6", "wxs", "tumoral", "tenant1"),
]

_SOMATIC_OCC_ROWS = [
    _occurrence(0, 11, 301, _A),
    _occurrence(1, 12, 302, _B),
    _occurrence(0, 13, 303, _D, filter_="LowQual"),
    _occurrence(0, 14, 304, _C),
    _occurrence(0, 16, 305, _E),
    _occurrence(0, 17, 306, _F),
]

_SOMATIC_FREQ_COLUMNS = ("pc_to_wgs", "pn_to_wgs", "pf_to_wgs", "pc_to_wxs", "pn_to_wxs", "pf_to_wxs")


@pytest.fixture
def somatic_frequencies(starrocks_session, radiant_mapping):
    for table_name in ("staging_sequencing_experiment", "somatic_cnv_occurrence", "somatic_cnv_variant_frequency"):
        _reset_table(starrocks_session, table_name, radiant_mapping)
    _seed(
        starrocks_session,
        radiant_mapping["starrocks_staging_sequencing_experiment"],
        _SOMATIC_SEQ_COLUMNS,
        _SOMATIC_SEQ_ROWS,
    )
    _seed(starrocks_session, radiant_mapping["starrocks_somatic_cnv_occurrence"], _OCC_COLUMNS, _SOMATIC_OCC_ROWS)
    _run_insert(starrocks_session, radiant_mapping, "somatic_cnv_variant_frequency_insert.sql")
    return _fetch(starrocks_session, radiant_mapping["starrocks_somatic_cnv_variant_frequency"], _SOMATIC_FREQ_COLUMNS)


def test_somatic_cnv_frequency(somatic_frequencies):
    assert sorted(somatic_frequencies) == [_A, _B, _C, _D, _E, _F]
    # p1, p2, p3 (non-PASS only) in wgs; p5, p6 in wxs. The tumor-normal p4 is in neither.
    assert {(r["pn_to_wgs"], r["pn_to_wxs"]) for r in somatic_frequencies.values()} == {(3, 2)}

    expected = {
        _A: (2, 0),  # A (p1) and B (p2); C's carrier is tumor-normal
        _B: (2, 0),
        _C: (2, 0),
        _D: (1, 0),  # B only: D's own carrier is non-PASS, A is one base too far
        _E: (0, 2),  # GAIN and GAINLOH pooled
        _F: (0, 2),
    }
    for cnv_id, (pc_to_wgs, pc_to_wxs) in expected.items():
        row = somatic_frequencies[cnv_id]
        assert (row["pc_to_wgs"], row["pc_to_wxs"]) == (pc_to_wgs, pc_to_wxs), f"cnv {cnv_id}"
        assert row["pf_to_wgs"] == pytest.approx(pc_to_wgs / 3)
        assert row["pf_to_wxs"] == pytest.approx(pc_to_wxs / 2)
