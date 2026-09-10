"""Render `pending_quality_control_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_TEMPLATE = "pending_quality_control_select.sql"
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
    sql = _render(_TEMPLATE, {"task_ids": [1], "tenants": ["radiant"]})
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def test_the_supersession_ctes_are_identical_to_the_annotation_query():
    """One policy, two templates: the newest completed experiment per member and the newest
    alignment per experiment must be selected the same way, or the two DAGs disagree on
    which experiment a case is 'about'."""
    qc = _without_comments(_render(_TEMPLATE))
    annotation = _without_comments(_render(_ANNOTATION_TEMPLATE))
    for cte in ("current_experiment", "current_alignment"):
        assert _cte(qc, cte) == _cte(annotation, cte), cte


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


def test_every_alignment_output_document_comes_back():
    """The metrics probe needs the directory of each output, whatever its type, so the
    final select joins all of them rather than picking one."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "LEFT JOIN alignment_document ad" in sql
    assert "ad.url                          AS document_url" in sql
    assert "cr.url                          AS cram_url" in sql
    assert "ci.url                          AS crai_url" in sql


def test_members_without_sequencing_come_back_carrying_a_reason():
    sql = _without_comments(_render(_TEMPLATE))
    for reason in ("pending_sequencing", "pending_alignment", "no_cram", "ambiguous_cram", "no_project_code"):
        assert f"'{reason}'" in sql
    assert "LEFT JOIN current_experiment ce" in sql


def test_only_germline_cases_and_never_revoked_ones():
    sql = _without_comments(_render(_TEMPLATE))
    assert "c.case_type_code = 'germline'" in sql
    assert "c.status_code IN ('in_progress', 'completed')" in sql
