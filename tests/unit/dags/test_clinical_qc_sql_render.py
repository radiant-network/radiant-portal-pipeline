"""Render the quality-control queries the way Airflow will, and pin what they must say."""

import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}

FACTS = "quality_control_facts_select.sql"
SOMATIC_FACTS = "quality_control_somatic_facts_select.sql"
MEMBERS = "quality_control_case_members_select.sql"
SOMATIC_MEMBERS = "quality_control_somatic_case_members_select.sql"
ALL = [FACTS, SOMATIC_FACTS, MEMBERS, SOMATIC_MEMBERS]


def _render(filename: str) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF), params={}
    )


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _output_columns(sql: str) -> list[str]:
    final_select = sql.split("\nSELECT ")[-1].split("\nFROM ")[0]
    return re.findall(r"\bAS (\w+),?$", final_select, flags=re.MULTILINE)


@pytest.mark.parametrize("filename", ALL)
def test_every_mapping_key_used_exists(filename):
    assert "{{" not in _render(filename)


@pytest.mark.parametrize("filename", ALL)
def test_no_stray_percent_survives_parameter_binding(filename):
    """`cursor.execute(sql, parameters)` runs `sql % params` over the whole statement, so
    every literal `%` -- comments included -- must be doubled (SJRA-1843 annex C)."""
    assert "%" not in re.sub(r"%\(\w+\)s", "", _render(filename)).replace("%%", "")


@pytest.mark.parametrize("filename", [FACTS, SOMATIC_FACTS])
def test_the_facts_take_no_parameter(filename):
    """The scheduler asks for every candidate; the tenant block list is decided in Python, as a reason."""
    sql = _without_comments(_render(filename))
    assert "%(" not in sql
    assert "WHERE pc.tenant_code" not in sql
    assert "exclusion_reason" not in sql


@pytest.mark.parametrize("filename", ALL)
def test_the_supersession_policy_is_read_from_the_shared_views(filename):
    """The scheduler decides and the child runs on the same experiment and the same alignment."""
    sql = _without_comments(_render(filename))
    assert "radiant.current_experiment ce" in sql
    assert "FROM radiant.current_alignment" in sql
    assert "FROM radiant.pipeline_case pc" in sql
    assert "status_code" not in sql
    assert "ROW_NUMBER" not in sql


@pytest.mark.parametrize(("filename", "case_type"), [(FACTS, "germline"), (MEMBERS, "germline")])
def test_a_germline_query_reads_the_germline_alignment_and_every_member(filename, case_type):
    sql = _without_comments(_render(filename))
    assert f"WHERE pc.case_type = '{case_type}'" in sql
    assert "JOIN radiant.case_member cm" in sql
    assert "'alignment_germline_variant_calling'" in sql
    assert "'alignment_somatic_variant_calling'" not in sql


@pytest.mark.parametrize("filename", [SOMATIC_FACTS, SOMATIC_MEMBERS])
def test_a_somatic_query_reads_the_proband_s_tumor_and_its_somatic_alignment(filename):
    sql = _without_comments(_render(filename))
    assert "WHERE pc.case_type = 'somatic'" in sql
    assert "AND ce.histology_code = 'tumoral'" in sql
    assert "AND pc.proband_id = ce.patient_id" in sql
    assert "WHERE task_type = 'alignment_somatic_variant_calling'" in sql
    # The patient directly: a somatic case can have no family row.
    assert "ON p.id = pc.proband_id" in sql


@pytest.mark.parametrize("filename", ALL)
def test_documents_are_selected_on_type_fields_never_on_file_name(filename):
    sql = _without_comments(_render(filename))
    assert "'ssup'" in sql and "'tgz'" in sql
    assert "LIKE" not in sql


def test_a_germline_candidate_has_a_cram_not_quality_controlled_for_the_case():
    sql = _without_comments(_render(FACTS))
    controlled = sql.split("quality_controlled AS (")[1].split("candidate AS (")[0]
    assert "t.task_type_code = 'quality_control_metrics'" in controlled
    assert "tc.case_id IS NOT NULL" in controlled
    candidate = sql.split("candidate AS (")[1].split("\n)\n")[0]
    assert "AND dc.cram_matches > 0" in candidate
    assert "AND qc.seq_id IS NULL" in candidate
    assert "qc.seq_id IS NOT NULL               AS annotated" in sql
    assert "LEFT JOIN (SELECT DISTINCT case_id FROM quality_controlled) ac ON ac.case_id = pc.case_id" in sql


def test_a_germline_member_comes_back_without_sequencing_so_its_reason_is_decided():
    sql = _without_comments(_render(FACTS))
    assert "LEFT JOIN radiant.current_experiment ce" in sql
    assert "LEFT JOIN radiant.current_alignment ca" in sql


def test_a_somatic_candidate_is_an_aligned_tumor_not_quality_controlled():
    """Unchanged: a somatic case is not a candidate before its tumor is aligned."""
    sql = _without_comments(_render(SOMATIC_FACTS))
    assert "JOIN current_alignment ca                        ON ca.seq_id = ce.seq_id" in sql
    assert "AND qc.seq_id IS NULL" in sql
    assert "t.task_type_code = 'quality_control_metrics'" in sql
    assert "LEFT JOIN (SELECT DISTINCT case_id FROM quality_controlled) ac ON ac.case_id = pc.case_id" in sql


def test_the_facts_of_both_case_types_have_the_same_columns():
    assert _output_columns(_render(FACTS)) == _output_columns(_render(SOMATIC_FACTS))


@pytest.mark.parametrize("filename", [MEMBERS, SOMATIC_MEMBERS])
def test_the_members_are_those_of_the_named_cases(filename):
    """The child decides nothing: a named case runs, quality-controlled before or not."""
    sql = _without_comments(_render(filename))
    assert "AND pc.case_id IN %(case_ids)s" in sql
    assert "quality_control_metrics" not in sql
    assert "candidate" not in sql
    for column in ("cram_url", "crai_url", "dragen_metrics_url", "current_alignment_task_id", "sample_id"):
        assert f"AS {column}" in sql


def test_the_members_of_both_case_types_have_the_same_columns():
    """`select_run_cases` reads either; the samplesheet tells them apart by `case_type`."""
    assert _output_columns(_render(MEMBERS)) == _output_columns(_render(SOMATIC_MEMBERS))


def test_a_tumor_s_cram_plays_no_role():
    sql = _without_comments(_render(SOMATIC_MEMBERS))
    assert "CAST(NULL AS VARCHAR)                   AS cram_url" in sql
    assert "'cram'" not in sql
