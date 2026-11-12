"""Render the four RAD-287 views the way the Init StarRocks Base Tables DAG will, and pin the rules they hold.

`pipeline_case`, `current_experiment`, `current_alignment` and `case_member` are the case filter and the
supersession policy (SJRA-1698 5) that every pending query and the staging view read. What each view returns
over real data is in `tests/integration/dags/sql/test_pipeline_views.py`.
"""

import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.dags.init_starrocks_tables import _BASE_TABLES
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_INIT_SQL = DAGS_DIR / "sql" / "radiant" / "init"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_VIEWS = ["pipeline_case", "current_experiment", "current_alignment", "case_member"]


def _render(view: str) -> str:
    text = (_INIT_SQL / f"{view}_create_table.sql").read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


@pytest.mark.parametrize("view", _VIEWS)
def test_every_mapping_key_used_exists(view):
    assert "{{" not in _render(view)


@pytest.mark.parametrize("view", _VIEWS)
def test_the_view_is_created_in_the_base_database(view):
    assert f"CREATE VIEW IF NOT EXISTS radiant.{view} AS" in _render(view)


def test_the_views_are_created_in_dependency_order_before_the_staging_view():
    """The init DAG chains its tasks in list order; a view must exist before a view reading it."""
    positions = [_BASE_TABLES.index(name) for name in [*_VIEWS, "staging_external_sequencing_experiment"]]
    assert positions == sorted(positions)


def test_pipeline_case_leaves_out_the_ignored_statuses():
    """A revoked case is often one left alone on purpose: admitting it would resurrect every one nightly."""
    sql = _without_comments(_render("pipeline_case"))
    assert "WHERE c.status_code NOT IN ('draft', 'revoked')" in sql
    assert "case_type_code" in sql
    assert "WHERE c.case_type_code" not in sql


def test_pipeline_case_joins_the_project_on_the_primary_key_alone():
    """`project.code` is globally unique and a project's tenant_code need not equal the case's: qualifying the
    join with it can only turn a match into a NULL, which fails the batch PATCH."""
    sql = _without_comments(_render("pipeline_case"))
    assert "LEFT JOIN radiant_jdbc.public.project pr ON pr.id = c.project_id\n" in sql
    assert "pr.tenant_code" not in sql


def test_current_experiment_keeps_the_newest_completed_experiment_per_case_and_member():
    """`completed` is a whitelist: unfinished sequencing is not a candidate, whatever the reason."""
    sql = _without_comments(_render("current_experiment"))
    assert "WHERE se.status_code = 'completed'" in sql
    assert "ORDER BY se.created_on DESC, se.id DESC" in sql
    assert "WHERE rn = 1" in sql
    assert "JOIN radiant.pipeline_case pc" in sql


def test_current_experiment_ranks_a_somatic_case_per_histology_and_a_germline_case_per_member():
    """A somatic proband keeps a current tumor and a current normal; for a germline case the third key is NULL."""
    sql = _without_comments(_render("current_experiment"))
    partition = re.search(r"PARTITION BY (.*?)\s+ORDER BY", sql, flags=re.DOTALL).group(1)
    keys = [key.strip() for key in partition.split(",")]
    assert keys == ["pc.case_id", "s.patient_id", "CASE WHEN pc.case_type = 'somatic' THEN s.histology_code END"]


def test_current_alignment_keeps_the_newest_task_per_experiment_and_type():
    sql = _without_comments(_render("current_alignment"))
    assert "PARTITION BY tc.sequencing_experiment_id, t.task_type_code" in sql
    assert "ORDER BY t.created_on DESC, t.id DESC" in sql
    assert "t.task_type_code IN ('alignment_germline_variant_calling', 'alignment_somatic_variant_calling')" in sql


def test_current_alignment_is_reached_through_the_experiment_not_the_case():
    """Alignment tasks carry a null `task_context.case_id`: joining on it returns an empty set, not an error."""
    sql = _without_comments(_render("current_alignment"))
    assert "tc.sequencing_experiment_id" in sql
    assert "tc.case_id" not in sql


def test_case_member_is_one_row_per_family_row_of_a_pipeline_case():
    sql = _without_comments(_render("case_member"))
    assert "FROM radiant_jdbc.public.family f" in sql
    assert "JOIN radiant.pipeline_case pc ON pc.case_id = f.case_id" in sql
    assert "f.id                           AS family_row_id" in sql
