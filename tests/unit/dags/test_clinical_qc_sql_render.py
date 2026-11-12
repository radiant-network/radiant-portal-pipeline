"""Render `pending_quality_control_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_TEMPLATE = "pending_quality_control_select.sql"
_NO_PARAMS = {"task_ids": [], "excluded_tenants": []}


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
    assert "%(excluded_tenants)s" not in sql


def test_named_tasks_narrow_candidacy_not_membership():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [1], "excluded_tenants": []}))
    assert sql.count("%(task_ids)s") == 1
    assert "%(task_ids)s" in sql.split("candidate AS (")[1].split("SELECT gc.case_id")[0]


def test_the_tenant_block_list_is_a_reason_not_a_filter():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [], "excluded_tenants": ["radiant"]}))
    assert "'tenant_excluded'" in sql
    assert "WHERE gc.tenant_code" not in sql


def test_no_stray_percent_survives_parameter_binding():
    sql = _render(_TEMPLATE, {"task_ids": [1], "excluded_tenants": ["radiant"]})
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def test_the_supersession_policy_is_read_from_the_shared_views():
    """One policy, every pending query: the newest completed experiment per member and the
    newest alignment per experiment come from the `current_experiment` and `current_alignment`
    views (RAD-287), so the DAGs cannot disagree on which experiment a case is 'about'."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "FROM radiant.current_experiment ce" in sql
    assert "FROM radiant.current_alignment" in sql
    assert "task_type = 'alignment_germline_variant_calling'" in _cte(sql, "current_alignment")
    assert "ROW_NUMBER" not in sql


def test_the_trigger_document_is_the_cram_selected_on_type_fields():
    sql = _without_comments(_render(_TEMPLATE))
    cram = _cte(sql, "cram")
    assert "data_type_code = 'alignment'" in cram
    assert "format_code    = 'cram'" in cram
    assert "LIKE" not in sql


def test_the_anti_join_is_on_the_qc_task_type_and_scoped_to_the_case():
    sql = _without_comments(_render(_TEMPLATE))
    done = _cte(sql, "quality_controlled")
    assert "t.task_type_code = 'quality_control_metrics'" in done
    assert "tc.case_id IS NOT NULL" in done
    assert "radiant_germline_annotation" not in sql


def test_the_dragen_metrics_tarball_is_selected_on_type_fields_and_counted():
    sql = _without_comments(_render(_TEMPLATE))
    tarball = _cte(sql, "dragen_metrics")
    assert "data_type_code = 'ssup'" in tarball
    assert "format_code    = 'tgz'" in tarball
    assert "COUNT(DISTINCT url) AS matches" in tarball
    assert "dm.url                          AS dragen_metrics_url" in sql
    assert "WHEN dm.matches > 1     THEN 'ambiguous_dragen_metrics'" in sql


def test_one_row_per_member_not_per_document():
    """Each joined CTE has at most one row per experiment; joining the raw documents would
    multiply the members."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "LEFT JOIN alignment_document" not in sql
    assert "cr.url                          AS cram_url" in sql
    assert "ci.url                          AS crai_url" in sql


def test_members_without_sequencing_come_back_carrying_a_reason():
    sql = _without_comments(_render(_TEMPLATE))
    for reason in (
        "pending_sequencing",
        "pending_alignment",
        "no_cram",
        "ambiguous_cram",
        "ambiguous_dragen_metrics",
        "no_project_code",
    ):
        assert f"'{reason}'" in sql
    assert "LEFT JOIN radiant.current_experiment ce" in sql


def test_only_germline_cases_and_never_revoked_ones():
    """Revoked and draft cases are left out by `pipeline_case` (RAD-287); the query keeps the
    germline ones."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "FROM radiant.pipeline_case pc" in sql
    assert "WHERE pc.case_type = 'germline'" in sql
    assert "WHERE ce.case_type = 'germline'" in sql
    assert "status_code" not in sql


def test_the_case_type_is_returned():
    assert "'germline'                      AS case_type" in _without_comments(_render(_TEMPLATE))
