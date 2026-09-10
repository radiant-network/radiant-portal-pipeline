import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}

_TEMPLATES = ["pending_annotation_select.sql", "case_phenotypes_select.sql"]

# What Airflow puts in the Jinja context. The discovery query branches on both, so every
# render has to say which branch it is exercising.
_NO_PARAMS = {"task_ids": [], "tenants": []}


def _render(filename: str, params: dict | None = None) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF),
        params=params if params is not None else _NO_PARAMS,
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


def test_discovery_is_unscoped_by_default():
    """A scheduled run supplies no ids and no tenants, and must then look at everything."""
    sql = _without_comments(_render("pending_annotation_select.sql"))
    assert "%(task_ids)s" not in sql
    assert "%(tenants)s" not in sql


def test_named_tasks_narrow_candidacy_not_membership():
    """`task_ids` restricts which cases become candidates. It must not reach the member
    rows, or a shared alignment task would return a family missing everyone else."""
    sql = _without_comments(_render("pending_annotation_select.sql", {"task_ids": [1], "tenants": []}))
    assert sql.count("%(task_ids)s") == 1
    # Not split on ")": `%(task_ids)s` carries one, and the naive split truncates on it.
    candidate_block = sql.split("candidate AS (")[1].split("SELECT gc.case_id")[0]
    assert "%(task_ids)s" in candidate_block


def test_the_tenant_allow_list_is_a_reason_not_a_filter():
    """An ungranted tenant is *reported*, not hidden -- otherwise its cases would go missing
    with nothing saying why the annotation never happened."""
    sql = _without_comments(_render("pending_annotation_select.sql", {"task_ids": [], "tenants": ["radiant"]}))
    assert "'tenant_not_granted'" in sql
    assert "WHERE gc.tenant_code" not in sql


@pytest.mark.parametrize("filename", _TEMPLATES)
def test_no_stray_percent_survives_parameter_binding(filename):
    """RadiantStarRocksOperator ends at `cursor.execute(sql, parameters)`, and the driver
    then runs `sql % params` over the *whole* statement. Any percent sign that is not part
    of `%%` or a `%(name)s` placeholder raises there -- including one inside a comment,
    which is exactly how this test earned its place."""
    sql = _render(filename, {"task_ids": [1], "tenants": ["radiant"]} if "pending" in filename else None)
    without_placeholders = re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")
    assert "%" not in without_placeholders


def test_one_experiment_per_member_and_one_alignment_per_experiment():
    """The supersession rule, and the reason it is a rule rather than a preference: the same
    selection has to feed eligibility and the family, or a discarded experiment keeps its
    case eligible for ever and the pipeline re-runs it nightly."""
    sql = _without_comments(_render("pending_annotation_select.sql"))
    assert "PARTITION BY chse.case_id, s.patient_id" in sql
    assert "PARTITION BY tc.sequencing_experiment_id" in sql
    assert sql.count("ORDER BY se.created_on DESC, se.id DESC") == 1
    assert sql.count("ORDER BY t.created_on DESC, t.id DESC") == 1


def test_only_completed_sequencing_is_annotable():
    """A whitelist, not a `revoke` blacklist: sequencing that is not finished is not a
    candidate, whatever the reason it is not finished."""
    assert "se.status_code = 'completed'" in _without_comments(_render("pending_annotation_select.sql"))


def test_the_gvcf_count_is_taken_over_one_alignment_task():
    """This is what makes `ambiguous_gvcf` diagnostic. Counted across every alignment task,
    two gVCFs could mean a legitimate re-alignment or a mistyped index and the message had
    to guess; counted over the current task alone, it can only be the mistyped document."""
    sql = _without_comments(_render("pending_annotation_select.sql"))
    gvcf_block = sql.split("gvcf AS (")[1].split("annotated AS (")[0]
    assert "FROM current_alignment ca" in gvcf_block
    assert "COUNT(DISTINCT d.url) AS matches" in gvcf_block


def test_the_alignment_is_reached_through_the_experiment_not_the_case():
    """Alignment tasks carry a null `task_context.case_id`, so joining on it returns nothing
    at all -- not an error, just an empty set."""
    alignment_block = _without_comments(_render("pending_annotation_select.sql")).split("current_alignment AS (")[1]
    alignment_block = alignment_block.split("gvcf AS (")[0]
    assert "tc.sequencing_experiment_id" in alignment_block
    assert "tc.case_id" not in alignment_block


def test_the_annotation_anti_join_is_scoped_to_the_case():
    """Annotation tasks *do* carry a case id, and that is what keeps a sequencing experiment
    shared by two cases honest: annotating case 1 must not make case 2 look done."""
    sql = _without_comments(_render("pending_annotation_select.sql"))
    annotated_block = sql.split("annotated AS (")[1].split("candidate AS (")[0]
    assert "tc.case_id" in annotated_block
    assert "a.case_id = ce.case_id" in sql
    assert "a.seq_id  = ce.seq_id" in sql
    assert "WHERE a.seq_id IS NULL" in sql


def test_members_without_sequencing_come_back_carrying_a_reason():
    """LEFT, all three. A family silently short one member is the one failure mode worth
    more than the rest put together."""
    sql = _without_comments(_render("pending_annotation_select.sql"))
    assert "LEFT JOIN current_experiment ce" in sql
    assert "LEFT JOIN current_alignment ca" in sql
    assert "LEFT JOIN gvcf g" in sql
    assert "'pending_sequencing'" in sql
    assert "'pending_alignment'" in sql


def test_revoked_cases_are_never_resurrected():
    """In the field a revoked case is very often precisely a case left un-annotated on
    purpose, so admitting it would re-process every one of them nightly."""
    sql = _without_comments(_render("pending_annotation_select.sql"))
    assert "c.status_code IN ('in_progress', 'completed')" in sql


def test_the_project_join_is_on_the_primary_key_alone():
    """`cases.project_id` is a foreign key to `project.id`, so this already resolves to
    exactly one row. Qualifying it with tenant_code cannot disambiguate anything and does
    turn a match into a NULL -- `project.code` is globally unique, so a project row carries
    one tenant_code that need not equal the case's. A null project_code fails the batch."""
    statement = _without_comments(_render("pending_annotation_select.sql"))
    assert "ON pr.id = gc.project_id" in statement
    assert "pr.tenant_code" not in statement


def test_discovery_returns_the_tenant_it_did_not_ask_for():
    """The batch PATCH is addressed to this, so it has to come back with the rows."""
    assert "c.tenant_code       AS tenant_code" in _render("pending_annotation_select.sql")


def test_discovery_reads_the_clinical_catalog():
    sql = _render("pending_annotation_select.sql")
    assert "radiant_jdbc.public.`cases`" in sql
    assert "radiant_jdbc.public.task_has_document" in sql


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
    sql = _render("pending_annotation_select.sql")
    assert "d.data_type_code = 'snv'" in sql
    assert "d.format_code    = 'gvcf'" in sql


def test_only_germline_cases_are_returned():
    """The pipeline's `step: genotype` entry point assumes germline joint calling."""
    assert "c.case_type_code = 'germline'" in _render("pending_annotation_select.sql")
