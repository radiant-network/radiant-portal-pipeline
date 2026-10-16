"""Render `pending_somatic_annotation_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_TEMPLATE = DAGS_DIR / "sql" / "clinical" / "pending_somatic_annotation_select.sql"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_NO_PARAMS = {"task_ids": [], "excluded_tenants": []}


def _render(params: dict | None = None) -> str:
    return jinja2.Template(_TEMPLATE.read_text(), undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF),
        params=params if params is not None else _NO_PARAMS,
    )


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _cte(sql: str, name: str) -> str:
    return sql.split(f"{name} AS (")[1].split("\n),\n")[0]


def test_every_mapping_key_used_exists():
    assert "{{" not in _render()


def test_discovery_is_unscoped_by_default_and_needs_an_ssnv_vcf():
    sql = _without_comments(_render())
    assert "%(task_ids)s" not in sql
    assert "%(excluded_tenants)s" not in sql
    assert "AND s.task_id IS NOT NULL" in _cte(sql, "candidate")


def test_named_tasks_narrow_candidacy_and_report_a_task_without_ssnv():
    sql = _without_comments(_render({"task_ids": [70], "excluded_tenants": []}))
    candidate = _cte(sql, "candidate")
    assert sql.count("%(task_ids)s") == 1
    assert "ct.task_id IN %(task_ids)s" in candidate
    assert "s.task_id IS NOT NULL" not in candidate
    assert "'no_ssnv'" in sql


def test_the_tenant_block_list_is_a_reason_not_a_filter():
    sql = _without_comments(_render({"task_ids": [], "excluded_tenants": ["radiant"]}))
    assert "'tenant_excluded'" in sql
    assert "WHERE sc.tenant_code" not in sql


def test_no_stray_percent_survives_parameter_binding():
    sql = _render({"task_ids": [1], "excluded_tenants": ["radiant"]})
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def test_only_somatic_cases_and_calling_tasks_are_looked_at():
    sql = _without_comments(_render())
    assert "c.case_type_code = 'somatic'" in _cte(sql, "somatic_case")
    calling = _cte(sql, "task_experiment")
    for task_type in ("somatic_variant_calling", "tumor_only_variant_calling", "alignment_somatic_variant_calling"):
        assert f"'{task_type}'" in calling


def test_the_trigger_is_the_ssnv_vcf_selected_on_type_fields():
    sql = _without_comments(_render())
    ssnv = _cte(sql, "ssnv")
    assert "data_type_code = 'ssnv'" in ssnv
    assert "format_code    = 'vcf'" in ssnv
    assert "thd.type = 'output'" in ssnv
    assert "'scnv'" not in sql
    assert "LIKE" not in sql


def test_a_vcf_already_input_of_an_annotation_on_the_same_case_is_done():
    """Per case, not global: a registration that succeeded in one tenant must not hide another."""
    sql = _without_comments(_render())
    context = _cte(sql, "annotation_context")
    assert "t.task_type_code = 'radiant_somatic_annotation'" in context
    assert "tc.case_id IS NOT NULL" in context
    assert "thd.type = 'input'" in _cte(sql, "annotation_input")
    annotated = _cte(sql, "annotated")
    assert "ac.case_id AS case_id" in annotated
    assert "data_type_code = 'ssnv'" in annotated
    candidate = _cte(sql, "candidate")
    assert "a.case_id = ct.case_id" in candidate
    assert "WHERE a.task_id IS NULL" in candidate


def test_only_completed_experiments_run():
    assert "WHEN se.status_code <> 'completed' THEN 'pending_sequencing'" in _render()


def test_the_case_must_hold_every_experiment_of_the_task():
    """The portal refuses an input document produced by an experiment the case does not hold."""
    case_task = _cte(_without_comments(_render()), "case_task")
    assert "HAVING COUNT(DISTINCT te.seq_id) = ts.experiments" in case_task
    assert "case_id IS NOT NULL" not in case_task
