"""Render the germline CNV queries the way Airflow will, and pin what they must say."""

import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}

FACTS = "germline_cnv_facts_select.sql"
MEMBERS = "germline_cnv_case_members_select.sql"


def _render(filename: str) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF), params={}
    )


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


@pytest.mark.parametrize("filename", [FACTS, MEMBERS])
def test_every_mapping_key_used_exists(filename):
    assert "{{" not in _render(filename)


@pytest.mark.parametrize("filename", [FACTS, MEMBERS])
def test_no_stray_percent_survives_parameter_binding(filename):
    """`cursor.execute(sql, parameters)` runs `sql % params` over the whole statement, so
    every literal `%` -- comments included -- must be doubled (SJRA-1843 annex C)."""
    assert "%" not in re.sub(r"%\(\w+\)s", "", _render(filename)).replace("%%", "")


def test_the_facts_take_no_parameter():
    """The scheduler asks for every candidate; the tenant block list is decided in Python, as a reason."""
    sql = _without_comments(_render(FACTS))
    assert "%(" not in sql
    assert "WHERE pc.tenant_code" not in sql
    assert "exclusion_reason" not in sql


@pytest.mark.parametrize("filename", [FACTS, MEMBERS])
def test_the_supersession_policy_and_the_cnv_vcf_are_read_from_the_shared_views(filename):
    """The scheduler decides and the child runs on the same experiment and the same CNV VCF."""
    sql = _without_comments(_render(filename))
    assert "LEFT JOIN radiant.current_experiment ce" in sql
    assert "LEFT JOIN radiant.current_alignment ca" in sql
    assert "LEFT JOIN radiant.current_germline_gcnv g" in sql
    assert "ca.pipeline_name                AS alignment_pipeline" in sql
    assert "ROW_NUMBER" not in sql


@pytest.mark.parametrize("filename", [FACTS, MEMBERS])
def test_the_cases_and_members_come_from_the_shared_views(filename):
    sql = _without_comments(_render(filename))
    assert "FROM radiant.pipeline_case pc" in sql
    assert "JOIN radiant.case_member cm" in sql
    assert "status_code" not in sql
    assert "WHERE pc.case_type = 'germline'" in sql


def test_a_candidate_has_a_cnv_vcf_not_annotated_for_the_case():
    """A case where no member has a CNV VCF is not a candidate: CNVs not called, or joint-called upstream."""
    sql = _without_comments(_render(FACTS))
    annotated = sql.split("annotated AS (")[1].split("candidate AS (")[0]
    assert "t.task_type_code = 'radiant_germline_cnv_annotation'" in annotated
    assert "tc.case_id IS NOT NULL" in annotated
    candidate = sql.split("candidate AS (")[1].split("\n)\n")[0]
    assert "JOIN radiant.current_germline_gcnv g ON g.seq_id = ce.seq_id" in candidate
    assert "AND a.seq_id IS NULL" in candidate
    assert "an.seq_id IS NOT NULL           AS annotated" in sql


def test_the_members_are_those_of_the_named_cases_with_their_cram():
    """The child decides nothing. The CRAM is carried, never a reason: Python decides per family whether to pass it."""
    sql = _without_comments(_render(MEMBERS))
    assert "AND pc.case_id IN %(case_ids)s" in sql
    assert "annotated" not in sql
    assert "candidate" not in sql
    for column in ("gcnv_url", "cram_url", "crai_url", "current_alignment_task_id", "sample_id"):
        assert f"AS {column}" in sql


def test_the_gcnv_view_counts_over_the_current_germline_alignment_by_document_type():
    text = (DAGS_DIR / "sql" / "radiant" / "init" / "current_germline_gcnv_create_table.sql").read_text()
    sql = jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))
    assert "FROM radiant.current_alignment ca" in sql
    assert "WHERE ca.task_type = 'alignment_germline_variant_calling'" in sql
    assert "COUNT(DISTINCT d.url) AS matches" in sql
    assert "d.data_type_code = 'gcnv'" in sql
    assert "d.format_code    = 'vcf'" in sql
    assert "LIKE" not in sql
