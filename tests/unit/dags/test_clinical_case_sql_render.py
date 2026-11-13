import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}

_TEMPLATES = ["germline_snv_facts_select.sql", "case_phenotypes_select.sql"]


def _render(filename: str, **variables) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF), **variables
    )


def _without_comments(sql: str) -> str:
    """Just the statement. These templates carry a lot of prose, and a comment explaining
    why something is absent will otherwise satisfy an assertion that it is present."""
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


@pytest.mark.parametrize("filename", _TEMPLATES)
def test_every_mapping_key_used_exists(filename):
    """StrictUndefined turns a typo like `mapping.hpo_term` -- a key that does not exist --
    into a render failure here instead of a NULL-shaped surprise at run time."""
    assert "{{" not in _render(filename)


def test_the_fact_query_decides_nothing():
    """No reason, no candidate filter: the decisions are made in Python, where they are unit
    tested, and the scheduler's discovery gets every germline case."""
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    assert "%(" not in sql
    assert "exclusion_reason" not in sql
    assert "candidate" not in sql


def test_a_child_reads_the_decided_cases_with_every_member():
    """`case_ids` narrows the cases, never the members: a family short one member is a different job."""
    sql = _without_comments(_render("germline_snv_facts_select.sql", case_ids=[1, 2]))
    assert sql.count("%(case_ids)s") == 1
    assert "AND pc.case_id IN %(case_ids)s" in sql


@pytest.mark.parametrize("filename", _TEMPLATES)
def test_no_stray_percent_survives_parameter_binding(filename):
    """RadiantStarRocksOperator ends at `cursor.execute(sql, parameters)`, and the driver
    then runs `sql % params` over the *whole* statement. Any percent sign that is not part
    of `%%` or a `%(name)s` placeholder raises there -- including one inside a comment,
    which is exactly how this test earned its place."""
    sql = _render(filename)
    without_placeholders = re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")
    assert "%" not in without_placeholders


def test_the_supersession_policy_is_read_from_the_shared_views():
    """The same selection has to feed eligibility and the family, or a discarded experiment
    keeps its case eligible for ever and the pipeline re-runs it nightly. Both read the
    `current_experiment` view (RAD-287), whose rules `test_pipeline_views_sql_render.py` pins;
    the query ranks nothing itself."""
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    assert "LEFT JOIN radiant.current_experiment ce" in sql
    assert "ROW_NUMBER" not in sql


def test_the_gvcf_count_is_taken_over_one_alignment_task():
    """This is what makes `ambiguous_gvcf` diagnostic. Counted across every alignment task,
    two gVCFs could mean a legitimate re-alignment or a mistyped index and the message had
    to guess; counted over the current task alone, it can only be the mistyped document."""
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    gvcf_block = sql.split("gvcf AS (")[1].split("annotated AS (")[0]
    assert "FROM current_alignment ca" in gvcf_block
    assert "COUNT(DISTINCT d.url) AS matches" in gvcf_block


def test_the_alignment_is_the_current_germline_one():
    """The view ranks germline and somatic alignments apart; this query wants the germline one."""
    alignment_block = _without_comments(_render("germline_snv_facts_select.sql")).split("current_alignment AS (")[1]
    alignment_block = alignment_block.split("gvcf AS (")[0]
    assert "FROM radiant.current_alignment" in alignment_block
    assert "task_type = 'alignment_germline_variant_calling'" in alignment_block


def test_the_annotation_is_scoped_to_the_case():
    """Annotation tasks *do* carry a case id, and that is what keeps a sequencing experiment
    shared by two cases honest: annotating case 1 must not make case 2 look done."""
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    annotated_block = sql.split("annotated AS (")[1].split("SELECT pc.case_id")[0]
    assert "tc.case_id" in annotated_block
    assert "a.case_id = pc.case_id" in sql
    assert "AND a.seq_id = ce.seq_id" in sql
    assert "a.seq_id IS NOT NULL            AS annotated" in sql


def test_members_without_sequencing_come_back():
    """LEFT, all of them. A family silently short one member is the one failure mode worth
    more than the rest put together."""
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    assert "LEFT JOIN radiant.current_experiment ce" in sql
    assert "LEFT JOIN current_alignment ca" in sql
    assert "LEFT JOIN gvcf g" in sql
    assert "LEFT JOIN annotated a" in sql


def test_the_cases_and_their_project_come_from_the_pipeline_case_view():
    """Revoked and draft cases, and the project code, are `pipeline_case`'s (RAD-287): in the
    field a revoked case is very often one left un-annotated on purpose, so admitting it would
    re-process every one of them nightly. The query filters no status and joins no project."""
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    assert "FROM radiant.pipeline_case pc" in sql
    assert "status_code" not in sql
    assert "radiant_jdbc.public.project" not in sql
    assert "pc.project_code                 AS project_code" in sql


def test_the_members_come_from_the_case_member_view():
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    assert "JOIN radiant.case_member cm" in sql
    assert "radiant_jdbc.public.family" not in sql


def test_discovery_returns_the_tenant_it_did_not_ask_for():
    """The batch PATCH is addressed to this, so it has to come back with the rows."""
    assert "pc.tenant_code                  AS tenant_code" in _render("germline_snv_facts_select.sql")


def test_discovery_reads_the_clinical_catalog():
    sql = _render("germline_snv_facts_select.sql")
    assert "radiant_jdbc.public.task_has_document" in sql
    assert "radiant_jdbc.public.task_context" in sql


def test_phenotypes_joins_the_shared_hpo_dictionary():
    """`hpo_term` is shared open data in the base database, not a clinical table -- hence a
    cross-catalog join, and a LEFT one so an unknown code loses its label, not its row."""
    sql = _render("case_phenotypes_select.sql")
    assert "radiant_jdbc.public.obs_categorical" in sql
    assert "LEFT JOIN radiant.hpo_term" in sql


def test_phenotypes_are_scoped_by_case_id_alone():
    """`cases.id` is a single-column primary key over one shared clinical schema, so a case
    id already names its tenant."""
    sql = _render("case_phenotypes_select.sql")
    assert "%(case_ids)s" in sql
    assert "%(tenant)s" not in sql


def test_the_gvcf_is_selected_on_document_type_not_filename():
    """Naming conventions differ between callers; the type fields do not. The germline code
    is `snv` -- the dictionary has no `gsnv`, and `ssnv` is somatic."""
    sql = _render("germline_snv_facts_select.sql")
    assert "d.data_type_code = 'snv'" in sql
    assert "d.format_code    = 'gvcf'" in sql


def test_only_germline_cases_are_returned():
    """The pipeline's `step: genotype` entry point assumes germline joint calling."""
    sql = _without_comments(_render("germline_snv_facts_select.sql"))
    assert "WHERE pc.case_type = 'germline'" in sql


def test_the_import_view_ignores_the_same_statuses_as_the_discovery_queries():
    """Both read `pipeline_case`, so they cannot disagree on which cases exist."""
    text = (
        DAGS_DIR / "sql" / "radiant" / "init" / "staging_external_sequencing_experiment_create_table.sql"
    ).read_text()
    sql = jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))
    assert "JOIN radiant.pipeline_case pc" in sql
    assert "status_code" not in sql
