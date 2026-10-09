"""Render `case_status_select.sql` the way Airflow will, and pin what it must say."""

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}


def _render() -> str:
    text = (DAGS_DIR / "sql" / "clinical" / "case_status_select.sql").read_text()
    sql = jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def test_every_mapping_key_used_exists():
    assert "{{" not in _render()


def test_only_the_statuses_the_pipeline_moves_from_are_read():
    assert "WHERE c.status_code IN ('submitted', 'processing')" in _render()


def test_variants_are_imported_snv_or_cnv_vcfs_that_are_not_deleted():
    imported = _render().split("imported AS (")[1].split(")\nSELECT")[0]
    assert "radiant.staging_sequencing_experiment " in imported
    assert "ingested_at IS NOT NULL" in imported
    assert "NOT se.deleted" in imported
    assert "se.vcf_filepath IS NOT NULL OR se.cnv_vcf_filepath IS NOT NULL" in imported
    # An Exomiser-only row is not variant data.
    assert "exomiser" not in imported


def test_no_parameter_is_bound():
    """Unscoped: every processing case is evaluated, not only this run's."""
    assert "%(" not in _render()
