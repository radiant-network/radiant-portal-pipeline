import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}

FACTS = "germline_snv_facts_select.sql"
MEMBERS = "germline_snv_case_members_select.sql"
_TEMPLATES = [FACTS, MEMBERS, "case_phenotypes_select.sql"]

# What Airflow puts in the Jinja context. No germline SNV query branches on it any more.
_NO_PARAMS = {}


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


def test_the_facts_take_no_parameter():
    """The scheduler asks for every candidate; the tenant block list is decided in Python, as a reason."""
    sql = _without_comments(_render(FACTS))
    assert "%(" not in sql
    assert "WHERE pc.tenant_code" not in sql


@pytest.mark.parametrize("filename", _TEMPLATES)
def test_no_stray_percent_survives_parameter_binding(filename):
    """RadiantStarRocksOperator ends at `cursor.execute(sql, parameters)`, and the driver
    then runs `sql % params` over the *whole* statement. Any percent sign that is not part
    of `%%` or a `%(name)s` placeholder raises there -- including one inside a comment,
    which is exactly how this test earned its place."""
    without_placeholders = re.sub(r"%\(\w+\)s", "", _render(filename)).replace("%%", "")
    assert "%" not in without_placeholders


@pytest.mark.parametrize("filename", [FACTS, MEMBERS])
def test_the_supersession_policy_and_the_gvcf_are_read_from_the_shared_views(filename):
    """The scheduler decides and the child runs on the same experiment and the same gVCF: both read the
    `current_experiment` and `current_germline_gvcf` views, and rank nothing themselves."""
    sql = _without_comments(_render(filename))
    assert "LEFT JOIN radiant.current_experiment ce" in sql
    assert "LEFT JOIN radiant.current_germline_gvcf g" in sql
    assert "ROW_NUMBER" not in sql
    assert "task_has_document" not in sql


@pytest.mark.parametrize("filename", [FACTS, MEMBERS])
def test_the_cases_and_members_come_from_the_shared_views(filename):
    """Revoked and draft cases, and the project code, are `pipeline_case`'s (RAD-287)."""
    sql = _without_comments(_render(filename))
    assert "FROM radiant.pipeline_case pc" in sql
    assert "JOIN radiant.case_member cm" in sql
    assert "status_code" not in sql
    assert "radiant_jdbc.public.family" not in sql
    assert "WHERE pc.case_type = 'germline'" in sql


def test_the_facts_tell_no_alignment_from_no_gvcf():
    """`pending_alignment` waits, `missing_input` blocks: the facts carry the current germline alignment apart."""
    sql = _without_comments(_render(FACTS))
    assert "LEFT JOIN radiant.current_alignment ca" in sql
    assert "ca.task_type = 'alignment_germline_variant_calling'" in sql
    assert "AS current_alignment_task_id" in sql


def test_the_annotation_is_scoped_to_the_case():
    """Annotation tasks *do* carry a case id, and that is what keeps a sequencing experiment
    shared by two cases honest: annotating case 1 must not make case 2 look done."""
    sql = _without_comments(_render(FACTS))
    annotated_block = sql.split("annotated AS (")[1].split("candidate AS (")[0]
    assert "tc.case_id" in annotated_block
    assert "a.case_id = ce.case_id" in sql
    assert "a.seq_id  = ce.seq_id" in sql
    assert "(ce.seq_id IS NULL OR a.seq_id IS NULL)" in sql
    assert "an.seq_id IS NOT NULL           AS annotated" in sql


def test_the_members_are_those_of_the_named_cases_with_no_candidacy():
    """The child decides nothing: it reads the members of the cases it is given, and what the pipeline needs."""
    sql = _without_comments(_render(MEMBERS))
    assert "AND pc.case_id IN %(case_ids)s" in sql
    assert "annotated" not in sql
    assert "candidate" not in sql
    for column in ("gvcf_url", "sample_id", "primary_condition", "submitter_patient_id", "tenant_code"):
        assert f"AS {column}" in sql


def test_the_gvcf_view_counts_over_the_current_germline_alignment_by_document_type():
    """Counted over the current task alone, two gVCFs can only be a mistyped document. Selected on the type
    fields, never the file name: the germline code is `snv` (`ssnv` is somatic)."""
    text = (DAGS_DIR / "sql" / "radiant" / "init" / "current_germline_gvcf_create_table.sql").read_text()
    sql = jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))
    assert "FROM radiant.current_alignment ca" in sql
    assert "WHERE ca.task_type = 'alignment_germline_variant_calling'" in sql
    assert "COUNT(DISTINCT d.url) AS matches" in sql
    assert "d.data_type_code = 'snv'" in sql
    assert "d.format_code    = 'gvcf'" in sql
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


def test_the_import_view_ignores_the_same_statuses_as_the_discovery_queries():
    """Both read `pipeline_case`, so they cannot disagree on which cases exist."""
    text = (
        DAGS_DIR / "sql" / "radiant" / "init" / "staging_external_sequencing_experiment_create_table.sql"
    ).read_text()
    sql = jinja2.Template(text, undefined=jinja2.StrictUndefined).render(mapping=get_radiant_mapping(_CONF))
    assert "JOIN radiant.pipeline_case pc" in sql
    assert "status_code" not in sql
