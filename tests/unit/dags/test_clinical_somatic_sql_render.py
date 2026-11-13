"""Render `somatic_snv_facts_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_TEMPLATE = DAGS_DIR / "sql" / "clinical" / "somatic_snv_facts_select.sql"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}


def _render(**variables) -> str:
    return jinja2.Template(_TEMPLATE.read_text(), undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF), **variables
    )


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _cte(sql: str, name: str) -> str:
    return sql.split(f"{name} AS (")[1].split("\n),\n")[0]


def test_every_mapping_key_used_exists():
    assert "{{" not in _render()


def test_the_ctes_answer_one_question_each_in_order():
    """Four CTEs, each named for the question it answers, built in dependency order."""
    sql = _without_comments(_render())
    names = [
        "work_unit_experiment",
        "registration_case",
        "vcf_to_annotate",
        "already_registered",
    ]
    positions = [sql.index(f"{name} AS (") for name in names]
    assert positions == sorted(positions)
    assert re.findall(r"^(?:WITH )?(\w+) AS \($", sql, flags=re.MULTILINE) == names


def test_the_fact_query_decides_nothing():
    """No reason and no candidate filter: Python decides, and the scheduler gets every case."""
    sql = _without_comments(_render())
    assert "%(" not in sql
    assert "exclusion_reason" not in sql
    assert "a.task_id IS NOT NULL          AS annotated" in sql
    assert "COALESCE(v.matches, 0)         AS ssnv_matches" in sql
    assert "se.status_code                 AS seq_status" in sql


def test_a_child_reads_the_decided_cases_only():
    sql = _without_comments(_render(case_ids=[1]))
    assert sql.count("%(case_ids)s") == 2
    assert "WHERE rc.case_id IN %(case_ids)s" in sql
    assert "AND pc.case_id IN %(case_ids)s" in sql


def test_a_somatic_case_no_calling_task_belongs_to_comes_back_once():
    sql = _without_comments(_render())
    assert "UNION ALL" in sql
    assert "AND rcs.case_id IS NULL" in sql


def test_no_stray_percent_survives_parameter_binding():
    sql = _render(case_ids=[1])
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def test_only_somatic_calling_tasks_are_looked_at():
    calling = _cte(_without_comments(_render()), "work_unit_experiment")
    for task_type in ("somatic_variant_calling", "tumor_only_variant_calling", "alignment_somatic_variant_calling"):
        assert f"'{task_type}'" in calling
    assert "radiant_somatic_annotation" not in calling
    assert "COUNT(*) OVER (PARTITION BY task_id) AS task_experiments" in calling
    assert "SELECT DISTINCT" in calling


def test_the_case_is_somatic_active_and_holds_every_experiment_of_the_task():
    """The portal refuses an input document produced by an experiment the case does not hold."""
    case_task = _cte(_without_comments(_render()), "registration_case")
    # Revoked and draft cases are left out by `pipeline_case` (RAD-287).
    assert "JOIN radiant.pipeline_case pc" in case_task
    assert "AND pc.case_type = 'somatic'" in case_task
    assert "HAVING COUNT(DISTINCT te.seq_id) = te.task_experiments" in case_task
    assert "case_id IS NOT NULL" not in case_task


def test_the_trigger_is_the_ssnv_vcf_selected_on_type_fields():
    sql = _without_comments(_render())
    vcf = _cte(sql, "vcf_to_annotate")
    assert "data_type_code = 'ssnv'" in vcf
    assert "format_code    = 'vcf'" in vcf
    assert "thd.type = 'output'" in vcf
    assert "'scnv'" not in sql
    assert "LIKE" not in sql


def test_a_vcf_already_input_of_an_annotation_on_the_same_case_is_done():
    """Per case, not global: a registration that succeeded in one tenant must not hide another."""
    sql = _without_comments(_render())
    annotated = _cte(sql, "already_registered")
    assert "t.task_type_code = 'radiant_somatic_annotation'" in annotated
    assert "tc.case_id IS NOT NULL" in annotated
    assert "ann_in.type = 'input'" in annotated
    assert "call_out.document_id = ann_in.document_id" in annotated
    assert "call_out.type = 'output'" in annotated
    assert "ON a.case_id = rc.case_id" in sql
