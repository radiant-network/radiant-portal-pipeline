import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}

_TEMPLATES = ["case_members_select.sql", "case_phenotypes_select.sql"]


def _render(filename: str) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))


def _without_comments(sql: str) -> str:
    """Just the statement. These templates carry a lot of prose, and a comment explaining
    why something is absent will otherwise satisfy an assertion that it is present."""
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


@pytest.mark.parametrize("filename", _TEMPLATES)
def test_every_mapping_key_used_exists(filename):
    """StrictUndefined turns a typo like `mapping.hpo_term` -- a key that does not exist --
    into a render failure here instead of a NULL-shaped surprise at run time."""
    assert "{{" not in _render(filename)


@pytest.mark.parametrize("filename", _TEMPLATES)
def test_the_case_ids_are_the_only_bound_parameter(filename):
    """`cases.id` is a single-column primary key over one shared clinical schema, so a case
    id already names its tenant. Filtering on a tenant as well would only add a way to name
    a correct case and get silence back."""
    sql = _render(filename)
    assert "%(case_ids)s" in sql
    assert "%(tenant)s" not in sql


def test_the_project_join_is_on_the_primary_key_alone():
    """`cases.project_id` is a foreign key to `project.id`, so this already resolves to
    exactly one row. Qualifying it with tenant_code cannot disambiguate anything and does
    turn a match into a NULL -- `project.code` is globally unique, so a project row carries
    one tenant_code that need not equal the case's. A null project_code fails the batch."""
    statement = _without_comments(_render("case_members_select.sql"))
    assert "ON pr.id = c.project_id" in statement
    assert "pr.tenant_code" not in statement


def test_members_returns_the_tenant_it_did_not_ask_for():
    """The batch PATCH is addressed to this, so it has to come back with the rows."""
    assert "c.tenant_code" in _render("case_members_select.sql")


def test_members_reads_the_clinical_catalog():
    sql = _render("case_members_select.sql")
    assert "radiant_jdbc.public.`cases`" in sql
    assert "radiant_jdbc.public.task_has_document" in sql


def test_phenotypes_joins_the_shared_hpo_dictionary():
    """`hpo_term` is shared open data in the base database, not a clinical table -- hence a
    cross-catalog join, and a LEFT one so an unknown code loses its label, not its row."""
    sql = _render("case_phenotypes_select.sql")
    assert "radiant_jdbc.public.obs_categorical" in sql
    assert "LEFT JOIN radiant.hpo_term" in sql


@pytest.mark.parametrize("filename", _TEMPLATES)
def test_no_stray_percent_survives_parameter_binding(filename):
    """RadiantStarRocksOperator ends at `cursor.execute(sql, parameters)`, and the driver
    then runs `sql % params` over the *whole* statement. Any percent sign that is not part
    of `%%` or a `%(name)s` placeholder raises there -- including one inside a comment,
    which is exactly how this test earned its place."""
    sql = _render(filename)
    without_placeholders = re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")
    assert "%" not in without_placeholders


def test_the_gvcf_is_selected_on_document_type_not_filename():
    """Naming conventions differ between callers; the type fields do not. The germline code
    is `snv` -- the dictionary has no `gsnv`, and `ssnv` is somatic."""
    sql = _render("case_members_select.sql")
    assert "d.data_type_code = 'snv'" in sql
    assert "d.format_code    = 'gvcf'" in sql


def test_only_germline_cases_are_returned():
    """The pipeline's `step: genotype` entry point assumes germline joint calling."""
    assert "c.case_type_code = 'germline'" in _render("case_members_select.sql")
