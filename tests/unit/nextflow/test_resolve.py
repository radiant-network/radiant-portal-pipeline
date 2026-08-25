import pytest

from radiant.tasks.nextflow.resolve import CaseResolutionError, resolve_families, tenant_of
from tests.unit.nextflow.conftest import member_row


def test_members_are_ordered_proband_first(trio_rows, phenotype_rows):
    """The PED and phenopacket writers read members positionally: `members[0]` is the
    proband, and the parents are looked up by role."""
    (family,) = resolve_families(trio_rows, phenotype_rows, [1072])
    assert [m.role for m in family.members] == ["proband", "father", "mother"]
    assert family.proband.sample_id == "NA12878"
    assert family.father.sample_id == "NA12891"
    assert family.mother.sample_id == "NA12892"


def test_family_id_is_ca_plus_the_case_id(trio_rows, phenotype_rows):
    """Not the bare numeric id: nf-schema coerces that to a number and then fails its own
    `type: string` check with a misleading message about spaces."""
    (family,) = resolve_families(trio_rows, phenotype_rows, [1072])
    assert family.family_id == "CA1072"


def test_only_the_probands_phenotypes_are_kept(trio_rows, phenotype_rows):
    (family,) = resolve_families(trio_rows, phenotype_rows, [1072])
    assert [p.hpo_id for p in family.phenotypes] == ["HP:0001249", "HP:0000618"]


def test_strategy_maps_to_the_pipelines_vocabulary(singleton_rows):
    """`wxs` and `wes` are the same thing under two dictionaries; the pipeline knows WES."""
    (family,) = resolve_families(singleton_rows, [], [8])
    assert family.sequencing_type == "WES"


def test_a_member_with_no_gvcf_is_rejected(trio_rows):
    """gvcf_matches = 0 means the case was joint-called upstream. There is nothing for
    `step: genotype` to do, so it must fail here rather than produce a short samplesheet."""
    trio_rows[0]["gvcf_matches"] = 0
    trio_rows[0]["gvcf_url"] = None
    with pytest.raises(CaseResolutionError, match="no gVCF registered"):
        resolve_families(trio_rows, [], [1072])


def test_an_ambiguous_gvcf_is_rejected(trio_rows):
    """More than one match means either a task spanning siblings ambiguously or a mistyped
    document. Either way the attribution is a guess, and a guess must not reach the run."""
    trio_rows[0]["gvcf_matches"] = 2
    with pytest.raises(CaseResolutionError, match="2 gVCFs match"):
        resolve_families(trio_rows, [], [1072])


def test_a_requested_case_that_resolves_to_nothing_is_named(trio_rows):
    """An unknown id and a somatic one both come back as silence, which would otherwise
    look like success."""
    with pytest.raises(CaseResolutionError, match=r"\[999\]"):
        resolve_families(trio_rows, [], [1072, 999])


def test_the_tenant_is_read_off_the_cases(trio_rows, phenotype_rows):
    """`cases.id` is globally unique, so the tenant is a consequence of the case ids rather
    than an input -- and the batch PATCH can only be addressed where the data came from."""
    (family,) = resolve_families(trio_rows, phenotype_rows, [1072])
    assert family.tenant_code == "radiant"
    assert tenant_of([family]) == "radiant"


def test_cases_spanning_two_tenants_are_rejected(trio_rows, singleton_rows):
    """One batch PATCH goes to one tenant. Registering half the run and failing the rest is
    worse than refusing and asking for two runs."""
    for row in singleton_rows:
        row["tenant_code"] = "other"
    with pytest.raises(CaseResolutionError, match="span several tenants"):
        resolve_families(trio_rows + singleton_rows, [], [1072, 8])


def test_a_case_without_exactly_one_proband_is_rejected(trio_rows):
    trio_rows.append(member_row(role="proband", patient_id=103, sample_id="NA12000", aliquot="NA12000"))
    with pytest.raises(CaseResolutionError, match="expected exactly 1 proband"):
        resolve_families(trio_rows, [], [1072])


def test_a_case_without_a_project_code_is_rejected(singleton_rows):
    """The batch PATCH resolves a case by (project_code, submitter_case_id) and 400s on a
    null. Catching it here costs a query; catching it at register_tasks costs a whole
    pipeline run first."""
    singleton_rows[0]["project_code"] = None
    with pytest.raises(CaseResolutionError, match="no project_code"):
        resolve_families(singleton_rows, [], [8])


def test_an_unsupported_strategy_is_rejected(singleton_rows):
    singleton_rows[0]["strategy"] = "rnaseq"
    with pytest.raises(CaseResolutionError, match="unsupported experimental strategy"):
        resolve_families(singleton_rows, [], [8])


def test_every_problem_is_reported_at_once(trio_rows, singleton_rows):
    """One round trip per run, not one per defect: these queries are slow and the operator
    is remote."""
    trio_rows[0]["gvcf_matches"] = 0
    singleton_rows[0]["strategy"] = "rnaseq"
    with pytest.raises(CaseResolutionError) as excinfo:
        resolve_families(trio_rows + singleton_rows, [], [1072, 8, 999])
    message = str(excinfo.value)
    assert "no gVCF registered" in message
    assert "unsupported experimental strategy" in message
    assert "[999]" in message
