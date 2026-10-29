import pytest

from radiant.tasks.vcf.experiment import Experiment, RadiantGermlineAnnotationTask
from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.snv.common import Common
from radiant.tasks.vcf.snv.germline.occurrence import INFO_FIELDS, process_occurrence
from radiant.tasks.vcf.vcf_utils import InfoKeys, resolve_info_keys
from tests.unit.vcf.vcf_test_utils import variant, vcf

# Declares VQSLOD as `VQSLod` and InbreedingCoeff under its alias `InbreedCoeff`, and no HaplotypeScore.
INFO_KEYS_VCF = "test_occurrence_info_keys.vcf"

task = RadiantGermlineAnnotationTask(
    task_id=1,
    part=1,
    analysis_type="germline",
    deleted=False,
    experiments=[
        Experiment(
            seq_id=1,
            patient_id=1,
            aliquot="SA0001",
            tenant_code="tenant1",
            family_role="proband",
            affected_status="affected",
            sex="F",
            experimental_strategy="wgs",
            request_priority="routine",
        )
    ],
    vcf_filepath="",
)
common = Common(task.task_id, task.part, "1-1000-A-G", "hash", "1", 1000, 1000, "A", "G")


@pytest.fixture
def info_keys():
    with vcf(INFO_KEYS_VCF) as v:
        return resolve_info_keys(v, INFO_FIELDS)


def test_resolve_ignores_case(info_keys):
    assert info_keys["VQSLOD"] == "VQSLod"
    assert info_keys["culprit"] == "culprit"


def test_resolve_falls_back_to_an_alias(info_keys):
    assert info_keys["InbreedingCoeff"] == "InbreedCoeff"


def test_resolve_marks_a_field_absent_from_the_header(info_keys):
    assert info_keys["HaplotypeScore"] is None
    assert "HaplotypeScore" in info_keys.missing()
    assert "VQSLOD" not in info_keys.missing()


def test_occurrence_reads_the_header_spelling(info_keys):
    record = variant(INFO_KEYS_VCF)
    occ = process_occurrence(record, Pedigree(task, ["SA0001"]), common, info_keys=info_keys)[1]
    assert occ["info_vqslod"] == pytest.approx(7.5)
    assert occ["info_culprit"] == "MQ"
    assert occ["info_inbreed_coeff"] == pytest.approx(-0.25)
    assert occ["info_qd"] == pytest.approx(12.5)
    assert occ["info_haplotype_score"] is None


def test_occurrence_without_keys_reads_the_names_as_is():
    record = variant(INFO_KEYS_VCF)
    occ = process_occurrence(record, Pedigree(task, ["SA0001"]), common)[1]
    assert occ["info_culprit"] == "MQ"
    assert occ["info_vqslod"] is None


def test_empty_info_keys_read_the_names_as_is():
    assert InfoKeys().value({"QD": 1.0}, "QD") == 1.0
