import pytest

from radiant.tasks.nextflow.resolve import (
    CaseResolutionError,
    resolve_families,
    select_cases,
    tenants_of,
)
from tests.unit.nextflow.conftest import member_row


def _kept(rows, requested=None, **kwargs):
    """The rows `select_cases` would hand on, as dicts, ready for `resolve_families`."""
    selection = select_cases(rows, requested, **kwargs)
    return [m.model_dump() for m in selection.members]


# --- building a family -------------------------------------------------------------------


def test_members_are_ordered_proband_first(trio_rows, phenotype_rows):
    """The PED and phenopacket writers read members positionally: `members[0]` is the
    proband, and the parents are looked up by role."""
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert [m.role for m in family.members] == ["proband", "father", "mother"]
    assert family.proband.sample_id == "NA12878"
    assert family.father.sample_id == "NA12891"
    assert family.mother.sample_id == "NA12892"


def test_family_id_is_ca_plus_the_case_id(trio_rows, phenotype_rows):
    """Not the bare numeric id: nf-schema coerces that to a number and then fails its own
    `type: string` check with a misleading message about spaces."""
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert family.family_id == "CA1072"


def test_only_the_probands_phenotypes_are_kept(trio_rows, phenotype_rows):
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert [p.hpo_id for p in family.phenotypes] == ["HP:0001249", "HP:0000618"]


def test_strategy_maps_to_the_pipelines_vocabulary(singleton_rows):
    """`wxs` and `wes` are the same thing under two dictionaries; the pipeline knows WES."""
    (family,) = resolve_families(_kept(singleton_rows), [])
    assert family.sequencing_type == "WES"


# --- lenient: a discovered scope ---------------------------------------------------------


def test_a_discovered_case_that_cannot_run_is_excluded_not_raised(trio_rows, singleton_rows):
    """The whole point of automation: one unfixable case must not block every other case,
    every night."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    selection = select_cases(trio_rows + singleton_rows)
    assert selection.case_ids == [8]
    assert [(e.case_id, e.reason) for e in selection.excluded] == [(1072, "no_gvcf")]


def test_the_exclusion_carries_a_sentence_not_only_a_code(trio_rows):
    """The report is read by someone who does not have the SQL open."""
    trio_rows[0]["exclusion_reason"] = "ambiguous_gvcf"
    (excluded,) = select_cases(trio_rows).excluded
    assert "mistyped at the source" in excluded.detail
    assert "index recorded with format_code 'gvcf'" in excluded.detail


def test_one_bad_member_excludes_the_whole_case(trio_rows):
    """The pipeline joint-calls whole families, so a family missing a member is not a
    smaller job -- it is a different one."""
    trio_rows[1]["exclusion_reason"] = "pending_alignment"
    selection = select_cases(trio_rows)
    assert selection.case_ids == []
    assert selection.members == []


def test_a_member_awaiting_sequencing_makes_the_case_wait(trio_rows):
    """Rather than running against the superseded experiment and producing an annotation
    that is obsolete the moment it lands."""
    trio_rows[1].update(seq_id=None, sample_id=None, aliquot=None, strategy=None, gvcf_url=None)
    trio_rows[1]["exclusion_reason"] = "pending_sequencing"
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason == "pending_sequencing"
    assert "no completed sequencing experiment yet" in excluded.detail


def test_a_case_without_exactly_one_proband_is_excluded(trio_rows):
    """After one experiment per member is selected this can only mean two different
    patients marked proband -- which is what the message now says."""
    trio_rows.append(member_row(role="proband", patient_id=103, sample_id="NA12000", aliquot="NA12000"))
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason == "proband_count"
    assert "two different patients are marked proband" in excluded.detail


def test_an_unsupported_strategy_is_excluded(singleton_rows):
    singleton_rows[0]["strategy"] = "rnaseq"
    (excluded,) = select_cases(singleton_rows).excluded
    assert excluded.reason == "unsupported_strategy"


def test_members_spanning_several_strategies_are_excluded(trio_rows):
    """A member whose newest experiment is a different strategy is not a re-sequencing."""
    trio_rows[0]["strategy"] = "wxs"
    (excluded,) = select_cases(trio_rows).excluded
    assert "span several strategies" in excluded.detail


# --- strict: an operator named the tasks -------------------------------------------------


def test_a_named_task_that_cannot_run_raises(trio_rows):
    """An operator asked for this specific work, so silence would be the wrong answer."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    with pytest.raises(CaseResolutionError, match="joint-called upstream"):
        select_cases(trio_rows, [900, 901, 902])


def test_a_named_task_that_produced_no_candidate_is_reported(trio_rows):
    """The query filters candidacy by task id, so an id that comes back empty is either
    unknown, not an alignment task, or already annotated everywhere it applies."""
    with pytest.raises(CaseResolutionError, match=r"\[999\]"):
        select_cases(trio_rows, [900, 901, 902, 999])


def test_strict_reports_every_problem_at_once(trio_rows, singleton_rows):
    """One round trip per run, not one per defect: these queries are slow and the operator
    is remote."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    singleton_rows[0]["strategy"] = "rnaseq"
    with pytest.raises(CaseResolutionError) as excinfo:
        select_cases(trio_rows + singleton_rows, [900, 901, 902, 910, 999])
    message = str(excinfo.value)
    assert "joint-called upstream" in message
    assert "unsupported experimental strategy" in message
    assert "[999]" in message


def test_a_discovery_run_is_lenient_by_default(trio_rows):
    """No task ids means nobody asked for anything in particular."""
    trio_rows[0]["exclusion_reason"] = "no_gvcf"
    assert select_cases(trio_rows).excluded  # does not raise


# --- tenants ------------------------------------------------------------------------------


def test_the_tenant_is_read_off_the_cases(trio_rows, phenotype_rows):
    """`cases.id` is globally unique, so the tenant is a consequence of the cases rather
    than an input -- and a batch can only be addressed where its data came from."""
    families = resolve_families(_kept(trio_rows), phenotype_rows)
    assert families[0].tenant_code == "radiant"
    assert tenants_of(families) == ["radiant"]


def test_a_run_may_span_several_tenants(trio_rows, singleton_rows):
    """One batch PATCH per tenant, not one pipeline run per tenant. Splitting the run would
    serialise hours of WGS behind the pipeline DAG's max_active_runs=1 for no reason: reads
    are one shared schema and the pipeline itself is tenant-blind."""
    for row in singleton_rows:
        row["tenant_code"] = "other"
    selection = select_cases(trio_rows + singleton_rows)
    assert selection.case_ids == [8, 1072]
    assert selection.tenants == ["other", "radiant"]

    families = resolve_families([m.model_dump() for m in selection.members], [])
    assert tenants_of(families) == ["other", "radiant"]


def test_an_ungranted_tenant_is_excluded_before_the_pipeline_runs(singleton_rows):
    """A 403 after hours of WGS is the expensive way to learn this."""
    singleton_rows[0]["exclusion_reason"] = "tenant_not_granted"
    (excluded,) = select_cases(singleton_rows).excluded
    assert "403" in excluded.detail
