"""Render the four RAD-287 views the way the Init StarRocks Base Tables DAG will.

Only what a render can show: every mapping key exists, the views land in the base database, and the init DAG
creates them before the staging view that reads them. The rules they hold (ignored statuses, newest completed
experiment, newest alignment, members) are checked on real data in `tests/integration/dags/sql/test_pipeline_views.py`
and the `*_case_resolution.py` tests.
"""

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
