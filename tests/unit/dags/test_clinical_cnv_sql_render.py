"""Render `germline_cnv_facts_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_TEMPLATE = "germline_cnv_facts_select.sql"


def _render(filename: str, **variables) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF), **variables
    )


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _cte(sql: str, name: str) -> str:
    return sql.split(f"{name} AS (")[1].split("\n),\n")[0]


def test_every_mapping_key_used_exists():
    assert "{{" not in _render(_TEMPLATE)


def test_the_fact_query_decides_nothing():
    """No reason and no candidate filter: Python decides, and the scheduler gets every case."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "%(" not in sql
    assert "exclusion_reason" not in sql
    assert "candidate" not in sql


def test_a_child_reads_the_decided_cases_with_every_member():
    sql = _without_comments(_render(_TEMPLATE, case_ids=[1]))
    assert sql.count("%(case_ids)s") == 1
    assert "AND pc.case_id IN %(case_ids)s" in sql


def test_no_stray_percent_survives_parameter_binding():
    """`cursor.execute(sql, parameters)` runs `sql % params` over the whole statement, so
    every literal `%` -- comments included -- must be doubled (SJRA-1843 annex C)."""
    sql = _render(_TEMPLATE, case_ids=[1])
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def test_the_supersession_policy_is_read_from_the_shared_views():
    """One policy, every pending query: the newest completed experiment per member and the
    newest alignment per experiment come from the `current_experiment` and `current_alignment`
    views (RAD-287), so the DAGs cannot disagree on which experiment a case is 'about'."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "LEFT JOIN radiant.current_experiment ce" in sql
    assert "FROM radiant.current_alignment" in sql
    assert "task_type = 'alignment_germline_variant_calling'" in _cte(sql, "current_alignment")
    assert "ROW_NUMBER" not in sql


def test_the_trigger_document_is_the_germline_cnv_vcf_selected_on_type_fields():
    sql = _without_comments(_render(_TEMPLATE))
    gcnv = _cte(sql, "gcnv")
    assert "data_type_code = 'gcnv'" in gcnv
    assert "format_code    = 'vcf'" in gcnv
    assert "COUNT(DISTINCT url) AS matches" in gcnv
    assert "LIKE" not in sql
    assert "'gvcf'" not in sql


def test_the_cram_is_carried_but_never_a_reason():
    """The CRAM feeds an optional pipeline step; Python decides per family whether to pass it."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "cr.url                          AS cram_url" in sql
    assert "ci.url                          AS crai_url" in sql
    assert "'no_cram'" not in sql


def test_the_alignment_pipeline_comes_back_for_the_caller_guard():
    sql = _without_comments(_render(_TEMPLATE))
    assert "ca.pipeline_name                AS alignment_pipeline" in sql


def test_the_annotation_is_the_cnv_annotation_task_scoped_to_the_case():
    sql = _without_comments(_render(_TEMPLATE))
    done = _cte(sql, "cnv_annotated")
    assert "t.task_type_code = 'radiant_germline_cnv_annotation'" in done
    assert "tc.case_id IS NOT NULL" in done
    assert "'radiant_germline_annotation'" not in sql
    assert "a.seq_id IS NOT NULL            AS annotated" in sql


def test_members_without_sequencing_come_back():
    sql = _without_comments(_render(_TEMPLATE))
    for join in ("radiant.current_experiment ce", "current_alignment ca", "gcnv g", "cnv_annotated a"):
        assert f"LEFT JOIN {join}" in sql


def test_only_germline_cases_and_never_revoked_ones():
    """Revoked and draft cases are left out by `pipeline_case` (RAD-287); the query keeps the
    germline ones."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "FROM radiant.pipeline_case pc" in sql
    assert "WHERE pc.case_type = 'germline'" in sql
    assert "status_code" not in sql
