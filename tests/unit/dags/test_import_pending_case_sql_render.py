"""Render `import_pending_case_select.sql` the way Airflow will, and pin what it must say."""

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}


def _render() -> str:
    text = (DAGS_DIR / "sql" / "radiant" / "import_pending_case_select.sql").read_text()
    sql = jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def test_every_mapping_key_used_exists():
    assert "{{" not in _render()


def test_a_case_is_pending_in_the_delta_or_inserted_but_not_yet_loaded():
    sql = _render()
    delta, staging = sql.split("UNION")
    assert "radiant.staging_sequencing_experiment_delta" in delta
    assert "radiant.staging_sequencing_experiment se" in staging


def test_the_staging_predicate_is_the_import_s_own():
    """`sequencing_experiment_select.sql` decides what the import runs; diverging from it would hold a
    case in `processing` the import never touches, or release one it still has to load."""
    import_select = (DAGS_DIR / "sql" / "radiant" / "sequencing_experiment_select.sql").read_text()
    predicate = import_select.split("WHERE", 1)[1].strip().rstrip(";")
    assert predicate.lower() in _render().lower()
