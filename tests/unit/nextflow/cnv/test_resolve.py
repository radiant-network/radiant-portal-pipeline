import pytest

from radiant.tasks.decision import Decision, Reason, ReasonCode
from radiant.tasks.journal import Pipeline
from radiant.tasks.nextflow.cnv.resolve import (
    CaseResolutionError,
    decide_cases,
    resolve_families,
    select_cases,
    select_run_cases,
    tenants_of,
)
from radiant.tasks.nextflow.inputs import build_ped
from tests.unit.nextflow.cnv.conftest import member_row


def _kept(rows, requested=None, **kwargs):
    selection = select_cases(rows, requested, **kwargs)
    return [m.model_dump() for m in selection.members]


# --- building a family -------------------------------------------------------------------


def test_members_are_ordered_proband_first(trio_rows, phenotype_rows):
    """The PED and phenopacket writers read members positionally."""
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert [m.role for m in family.members] == ["proband", "father", "mother"]
    assert family.proband.aliquot == "NA12878"
    assert family.father.aliquot == "NA12891"
    assert family.mother.aliquot == "NA12892"


def test_family_id_is_ca_plus_the_case_id(trio_rows, phenotype_rows):
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert family.family_id == "CA1072"


def test_only_the_probands_phenotypes_are_kept(trio_rows, phenotype_rows):
    (family,) = resolve_families(_kept(trio_rows), phenotype_rows)
    assert [p.hpo_id for p in family.phenotypes] == ["HP:0001249", "HP:0000618"]


def test_strategy_maps_to_the_pipelines_vocabulary(singleton_rows):
    """`wxs` and `wes` are the same thing under two dictionaries; the pipeline knows WES."""
    (family,) = resolve_families(_kept(singleton_rows), [])
    assert family.sequencing_type == "WES"


def test_a_family_with_every_cram_and_index_in_place_is_complete(trio_rows):
    (family,) = resolve_families(_kept(trio_rows), [])
    assert family.crams_complete is True


def test_a_family_missing_one_cram_is_not_complete(trio_rows):
    """All-or-nothing: the pipeline refines a family only when every sample has an
    alignment, so one missing CRAM makes the others useless for that step."""
    trio_rows[1].update(cram_url=None, crai_url=None)
    (family,) = resolve_families(_kept(trio_rows), [])
    assert family.crams_complete is False


def test_an_index_not_registered_beside_its_cram_makes_the_family_incomplete(trio_rows):
    """The pipeline resolves the index as `<cram>.crai` by file existence; anywhere else
    and mosdepth fails rather than skips."""
    trio_rows[0]["crai_url"] = "s3://qlin-nextflow-inputs/elsewhere/NA12892.crai"
    (family,) = resolve_families(_kept(trio_rows), [])
    assert family.crams_complete is False


# --- lenient: a discovered scope ---------------------------------------------------------


def test_a_discovered_case_that_cannot_run_is_excluded_not_raised(trio_rows, singleton_rows):
    trio_rows[0]["exclusion_reason"] = "no_gcnv"
    selection = select_cases(trio_rows + singleton_rows)
    assert selection.case_ids == [8]
    assert [(e.case_id, e.reason) for e in selection.excluded] == [
        (1072, Reason(code=ReasonCode.MISSING_INPUT, detail="gcnv", patient_id=102))
    ]


def test_the_exclusion_carries_a_sentence_not_only_a_code(trio_rows):
    trio_rows[0]["exclusion_reason"] = "ambiguous_gcnv"
    (excluded,) = select_cases(trio_rows).excluded
    assert "mistyped at the source" in excluded.message
    assert "more than one gcnv document" in excluded.message


def test_one_bad_member_excludes_the_whole_case(trio_rows):
    trio_rows[1]["exclusion_reason"] = "pending_alignment"
    selection = select_cases(trio_rows)
    assert selection.case_ids == []
    assert selection.members == []


def test_a_non_dragen_alignment_is_excluded_before_the_pipeline_runs(trio_rows):
    """The pipeline's normalization step errors on any caller but DRAGEN, on the very first
    process -- after the driver pod has been scheduled and the inputs staged."""
    trio_rows[2]["alignment_pipeline"] = "GATK"
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason.code == "unsupported_caller"
    assert excluded.reason == Reason(code=ReasonCode.UNSUPPORTED_CALLER, detail="GATK", patient_id=100)


def test_dragen_is_matched_loosely(trio_rows):
    """`Dragen` (the seeds' spelling) and `DRAGEN 4.2.4` (real data may carry a version) both pass."""
    trio_rows[0]["alignment_pipeline"] = "DRAGEN 4.2.4"
    assert select_cases(trio_rows).case_ids == [1072]


def test_an_alignment_without_a_pipeline_name_is_excluded_not_assumed_dragen(trio_rows):
    """`task.pipeline_name` is nullable. The samplesheet writes `caller=DRAGEN` as a fact, so an
    alignment that does not say what produced it fails closed, with its own reason."""
    trio_rows[1]["alignment_pipeline"] = None
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason == Reason(code=ReasonCode.UNSUPPORTED_CALLER, detail="unknown", patient_id=101)
    assert "no pipeline_name" in excluded.message


def test_a_member_still_awaiting_alignment_is_not_also_reported_as_unknown_caller(trio_rows):
    """One reason per cause: the query already says `pending_alignment`."""
    trio_rows[1].update(
        alignment_task_id=None, current_alignment_task_id=None, alignment_pipeline=None, gcnv_url=None, gcnv_matches=0
    )
    trio_rows[1]["exclusion_reason"] = "pending_alignment"
    selection = select_cases(trio_rows)
    assert [e.reason.code for e in selection.excluded] == ["pending_alignment"]


def test_a_member_without_an_aliquot_is_excluded(trio_rows):
    """The aliquot is every id the pipeline sees, and the `<proband>` segment of the Exomiser report
    names: a missing one is reported here, not found as `None` in the samplesheet hours later."""
    trio_rows[1]["aliquot"] = None
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason.code == "missing_aliquot"
    assert "patient 101" in excluded.message


def test_a_case_without_exactly_one_proband_is_excluded(trio_rows):
    trio_rows.append(member_row(role="proband", patient_id=103, aliquot="NA12000"))
    (excluded,) = select_cases(trio_rows).excluded
    assert excluded.reason.code == "proband_count"


def test_members_spanning_several_strategies_are_excluded(trio_rows):
    """The Exomiser analysis file is picked per family from `sequencingType`."""
    trio_rows[0]["strategy"] = "wxs"
    (excluded,) = select_cases(trio_rows).excluded
    assert "span several strategies" in excluded.message


# --- strict: an operator named the tasks -------------------------------------------------


def test_a_named_task_that_cannot_run_raises(trio_rows):
    trio_rows[0]["exclusion_reason"] = "no_gcnv"
    with pytest.raises(CaseResolutionError, match="has no gcnv: a document"):
        select_cases(trio_rows, [900, 901, 902])


def test_a_named_task_that_produced_no_candidate_is_reported(trio_rows):
    with pytest.raises(CaseResolutionError, match=r"\[999\].*radiant_germline_cnv_annotation"):
        select_cases(trio_rows, [900, 901, 902, 999])


def test_a_discovery_run_is_lenient_by_default(trio_rows):
    trio_rows[0]["exclusion_reason"] = "no_gcnv"
    assert select_cases(trio_rows).excluded  # does not raise


# --- tenants ------------------------------------------------------------------------------


def test_a_run_may_span_several_tenants(trio_rows, singleton_rows):
    for row in singleton_rows:
        row["tenant_code"] = "other"
    selection = select_cases(trio_rows + singleton_rows)
    assert selection.case_ids == [8, 1072]
    assert selection.tenants == ["other", "radiant"]
    families = resolve_families([m.model_dump() for m in selection.members], [])
    assert tenants_of(families) == ["other", "radiant"]


def test_an_excluded_tenant_is_left_out_before_the_pipeline_runs(singleton_rows):
    singleton_rows[0]["exclusion_reason"] = "tenant_excluded"
    (excluded,) = select_cases(singleton_rows).excluded
    assert excluded.reason.code == "tenant_excluded"
    assert "RADIANT_ETL_EXCLUDED_TENANTS" in excluded.message


# --- the scheduler's decision, on the facts --------------------------------------------------

# The father's facts once he is not sequenced yet.
_NOT_SEQUENCED = {"seq_id": None, "aliquot": None, "current_alignment_task_id": None, "gcnv_matches": 0}


def _father(rows):
    return next(row for row in rows if row["role"] == "father")


def _decided(rows, excluded_tenants=()):
    return [(e.case_id, e.decision, e.reasons) for e in decide_cases(rows, list(excluded_tenants))]


def _first_run(*patient_ids):
    return tuple(Reason(code=ReasonCode.FIRST_RUN, patient_id=p) for p in patient_ids)


def _new_sequencing(*patient_ids):
    return tuple(Reason(code=ReasonCode.NEW_SEQUENCING, patient_id=p) for p in patient_ids)


def _annotated_before(rows, *not_annotated_roles):
    for row in rows:
        row["case_annotated"] = True
        row["annotated"] = row["role"] not in not_annotated_roles


def test_a_first_run_names_every_member(trio_rows):
    (entry,) = decide_cases(trio_rows, [])
    assert (entry.pipeline, entry.decision, entry.reasons) == (
        Pipeline.GERMLINE_CNV,
        Decision.RUN,
        _first_run(100, 101, 102),
    )


def test_a_parent_not_sequenced_yet_does_not_block_the_proband(trio_rows):
    """The family runs with the members present; the reason names who is missing."""
    _father(trio_rows).update(_NOT_SEQUENCED)
    assert _decided(trio_rows) == [
        (1072, Decision.RUN, (*_first_run(100, 102), Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101)))
    ]


def test_after_the_partial_run_the_case_waits_for_the_absent_parent(trio_rows):
    _father(trio_rows).update(_NOT_SEQUENCED)
    _annotated_before(trio_rows, "father")
    assert _decided(trio_rows) == [
        (1072, Decision.WAIT, (Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101),))
    ]


def test_the_case_runs_again_once_the_parent_arrives_and_says_for_whom(trio_rows):
    _annotated_before(trio_rows, "father")
    assert _decided(trio_rows) == [(1072, Decision.RUN, _new_sequencing(101))]


def test_a_singleton_sequenced_again_is_a_new_sequencing(singleton_rows):
    _annotated_before(singleton_rows, "proband")
    assert _decided(singleton_rows) == [(8, Decision.RUN, _new_sequencing(200))]


def test_a_case_with_every_member_annotated_is_not_decided(trio_rows):
    for row in trio_rows:
        row["annotated"] = True
    assert decide_cases(trio_rows, []) == []


def test_a_non_dragen_alignment_blocks_the_case(trio_rows):
    _father(trio_rows)["alignment_pipeline"] = "GATK"
    assert _decided(trio_rows) == [
        (1072, Decision.BLOCKED, (Reason(code=ReasonCode.UNSUPPORTED_CALLER, detail="GATK", patient_id=101),))
    ]


def test_a_member_aligned_without_a_cnv_vcf_blocks_the_case(trio_rows):
    _father(trio_rows).update(gcnv_url=None, gcnv_matches=0)
    assert _decided(trio_rows) == [
        (1072, Decision.BLOCKED, (Reason(code=ReasonCode.MISSING_INPUT, detail="gcnv", patient_id=101),))
    ]


def test_an_excluded_tenant_is_skipped(singleton_rows):
    (entry,) = decide_cases(singleton_rows, ["radiant"])
    assert entry.decision == Decision.SKIP


# --- the child's check of the named cases ---------------------------------------------------


def test_the_child_runs_the_family_without_its_absent_parent(trio_rows):
    """The pipeline stops on a PED parent missing from the VCFs, so an absent parent is written `0`."""
    _father(trio_rows).update(_NOT_SEQUENCED, alignment_task_id=None, gcnv_url=None, cram_url=None, crai_url=None)
    selection = select_run_cases(trio_rows, [])
    assert selection.case_ids == [1072]
    assert [m.role for m in selection.members] == ["proband", "mother"]
    (family,) = resolve_families([m.model_dump() for m in selection.members], [])
    proband_line = build_ped(family).splitlines()[0].split("\t")
    assert proband_line[2:4] == ["0", "NA12892"]
    assert family.crams_complete is True


def test_the_child_leaves_out_a_case_whose_proband_is_absent(trio_rows):
    next(row for row in trio_rows if row["role"] == "proband").update(_NOT_SEQUENCED)
    selection = select_run_cases(trio_rows, [])
    assert selection.case_ids == []
    assert [e.reason.code for e in selection.excluded] == ["pending_sequencing"]
