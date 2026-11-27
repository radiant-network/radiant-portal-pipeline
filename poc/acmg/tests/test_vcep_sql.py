import pytest
from acmg.db import connect, execute, sql

DB = "acmg_poc_vcep_test"
CASES = "SELECT 7"
CLASSIFIED = "SELECT 1 AS locus_id, 'T1' AS transcript_id"


def criteria(*codes: str) -> str:
    """The panel's criteria as a SQL array of structs (code = criterion, no strength suffix)."""
    return "[" + ", ".join(f"row('{c.split('_')[0]}', '{c}', 'Strong', 4)" for c in codes) + "]"


def apply_panel(panel: list[str], ours: tuple[str, ...] = (), gnomad_an: int | None = 150_000) -> None:
    """Runs the VCEP step for a panel with these codes on a variant whose computed criteria are `ours`."""
    execute(f"DROP DATABASE IF EXISTS {DB} FORCE; CREATE DATABASE {DB}")
    execute(sql("create_tables.sql", output_db=DB))
    execute(
        f"CREATE TABLE {DB}.clinvar (locus_id BIGINT, name VARCHAR(20)) DUPLICATE KEY (locus_id)"
        " DISTRIBUTED BY HASH(locus_id) BUCKETS 1;\n"
        f"INSERT INTO {DB}.clinvar VALUES (1, '100');\n"
        f"CREATE TABLE {DB}.gnomad_genomes_v3 (locus_id BIGINT, an BIGINT) DUPLICATE KEY (locus_id)"
        " DISTRIBUTED BY HASH(locus_id) BUCKETS 1;\n"
        f"INSERT INTO {DB}.clingen_erepo (variation_id, criteria) VALUES ('100', {criteria(*panel)});\n"
        f"INSERT INTO {DB}.acmg_occurrence_criteria (seq_id, locus_id, criterion, code, strength, points, ruleset_version)"
        " VALUES (7, 1, 'PM3', 'PM3', 'Moderate', 2, 'x'), (7, 1, 'PM6', 'PM6', 'Supporting', 1, 'x'),"
        " (7, 1, 'BS4', 'BS4', 'Strong', -4, 'x'), (8, 1, 'PM3', 'PM3', 'Moderate', 2, 'x')"
    )
    if gnomad_an is not None:
        execute(f"INSERT INTO {DB}.gnomad_genomes_v3 VALUES (1, {gnomad_an})")
    if ours:
        rows = ", ".join(f"(1, 'T1', '{c}', '{c}', 'Strong', 0, 'gnomad', 'x')" for c in ours)
        execute(
            f"INSERT INTO {DB}.acmg_variant_criteria (locus_id, transcript_id, criterion, code, strength, points,"
            f" source, ruleset_version) VALUES {rows}"
        )
    params = {
        "shared_db": DB, "output_db": DB, "classified": CLASSIFIED, "cases": CASES, "ruleset_version": "x",
        "min_an": 2000,
    }  # fmt: skip
    execute(sql("vcep_criteria.sql", **params))


def rows(query: str) -> list:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(query)
        return [r[0] if len(r) == 1 else r for r in cur.fetchall()]


def occurrence_after(panel: list[str]) -> list[str]:
    """Criteria of the case left on the variant once a panel with these codes has been applied."""
    apply_panel(panel)
    return rows(f"SELECT criterion FROM {DB}.acmg_occurrence_criteria WHERE seq_id = 7 ORDER BY criterion")


def variant_after(panel: list[str], ours: tuple[str, ...], **kwargs) -> list[tuple[str, str]]:
    """(criterion, source) of the variant once the panel is applied over our computed `ours`."""
    apply_panel(panel, ours, **kwargs)
    return rows(f"SELECT criterion, source FROM {DB}.acmg_variant_criteria ORDER BY criterion")


@pytest.fixture(autouse=True, scope="module")
def local_starrocks():
    try:
        connect().close()
    except Exception:
        pytest.skip("no local StarRocks")
    yield
    execute(f"DROP DATABASE IF EXISTS {DB} FORCE")


def test_case_criteria_stay_when_the_panel_did_not_apply_them():
    assert occurrence_after(["PS3_Strong"]) == ["BS4", "PM3", "PM6"]


def test_panel_pm3_drops_ours_whatever_the_strength():
    assert occurrence_after(["PM3_Strong"]) == ["BS4", "PM6"]


def test_panel_ps2_drops_our_pm6():
    assert occurrence_after(["PS2_Strong"]) == ["BS4", "PM3"]
    assert occurrence_after(["PM6_Supporting"]) == ["BS4", "PM3"]


def test_frequency_contradiction_keeps_ours_panel_pm2_against_our_bs1():
    assert variant_after(["PM2_Supporting", "PS3_Strong"], ("BS1", "PP3")) == [
        ("BS1", "gnomad"), ("PS3", "clingen_vcep"),
    ]  # fmt: skip


def test_frequency_contradiction_keeps_ours_panel_ba1_against_our_pm2():
    assert variant_after(["BA1"], ("PM2", "PP3")) == [("PM2", "gnomad")]


def test_panel_ba1_wins_over_our_pm2_where_gnomad_is_thin():
    assert variant_after(["BA1"], ("PM2", "PP3"), gnomad_an=None) == [("BA1", "clingen_vcep")]
    assert variant_after(["BA1"], ("PM2", "PP3"), gnomad_an=1000) == [("BA1", "clingen_vcep")]


def test_no_frequency_contradiction_keeps_the_panels():
    # Same direction (panel BS1, ours BA1), the panel without a frequency criterion, and ours absent.
    assert variant_after(["BS1"], ("BA1", "PP3")) == [("BS1", "clingen_vcep")]
    assert variant_after(["PS3_Strong"], ("PM2", "PP3")) == [("PS3", "clingen_vcep")]
    assert variant_after(["PM2_Supporting"], ("PP3",)) == [("PM2", "clingen_vcep")]
