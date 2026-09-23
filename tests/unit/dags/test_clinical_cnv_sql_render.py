"""Render `pending_cnv_annotation_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_TEMPLATE = "pending_cnv_annotation_select.sql"
_ANNOTATION_TEMPLATE = "pending_annotation_select.sql"
_NO_PARAMS = {"task_ids": [], "tenants": []}


def _render(filename: str, params: dict | None = None) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF),
        params=params if params is not None else _NO_PARAMS,
    )


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _cte(sql: str, name: str) -> str:
    return sql.split(f"{name} AS (")[1].split("\n),\n")[0]


def test_every_mapping_key_used_exists():
    assert "{{" not in _render(_TEMPLATE)


def test_discovery_is_unscoped_by_default():
    sql = _without_comments(_render(_TEMPLATE))
    assert "%(task_ids)s" not in sql
    assert "%(tenants)s" not in sql


def test_named_tasks_narrow_candidacy_not_membership():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [1], "tenants": []}))
    assert sql.count("%(task_ids)s") == 1
    assert "%(task_ids)s" in sql.split("candidate AS (")[1].split("SELECT gc.case_id")[0]


def test_the_tenant_allow_list_is_a_reason_not_a_filter():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [], "tenants": ["radiant"]}))
    assert "'tenant_not_granted'" in sql
    assert "WHERE gc.tenant_code" not in sql


def test_no_stray_percent_survives_parameter_binding():
    """`cursor.execute(sql, parameters)` runs `sql % params` over the whole statement, so
    every literal `%` -- comments included -- must be doubled (SJRA-1843 annex C)."""
    sql = _render(_TEMPLATE, {"task_ids": [1], "tenants": ["radiant"]})
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def test_the_supersession_ctes_are_identical_to_the_annotation_query():
    """One policy, three templates: the newest completed experiment per member and the newest
    alignment per experiment must be selected the same way, or the DAGs disagree on which
    experiment a case is 'about'."""
    cnv = _without_comments(_render(_TEMPLATE))
    annotation = _without_comments(_render(_ANNOTATION_TEMPLATE))
    for cte in ("current_experiment", "current_alignment"):
        assert _cte(cnv, cte) == _cte(annotation, cte), cte


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
    assert "at.pipeline_name                AS alignment_pipeline" in sql


def test_the_anti_join_is_on_the_cnv_annotation_task_type_and_scoped_to_the_case():
    sql = _without_comments(_render(_TEMPLATE))
    done = _cte(sql, "cnv_annotated")
    assert "t.task_type_code = 'radiant_germline_cnv_annotation'" in done
    assert "tc.case_id IS NOT NULL" in done
    assert "'radiant_germline_annotation'" not in sql


def test_members_without_sequencing_come_back_carrying_a_reason():
    sql = _without_comments(_render(_TEMPLATE))
    for reason in ("pending_sequencing", "pending_alignment", "no_gcnv", "ambiguous_gcnv", "no_project_code"):
        assert f"'{reason}'" in sql
    assert "LEFT JOIN current_experiment ce" in sql


def test_only_germline_cases_and_never_revoked_ones():
    sql = _without_comments(_render(_TEMPLATE))
    assert "c.case_type_code = 'germline'" in sql
    assert "c.status_code IN ('in_progress', 'completed')" in sql
