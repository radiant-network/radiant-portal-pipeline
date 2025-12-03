import math

import pytest

from radiant.tasks.vcf.experiment import Experiment, RadiantGermlineAnnotationTask
from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.snv.germline.common import Common
from radiant.tasks.vcf.snv.germline.occurrence import (
    AUTOSOMAL_ORIGINS_LOOKUP,
    X_ORIGINS_LOOKUP,
    Y_ORIGINS_LOOKUP,
    adjust_calls_and_zygosity,
    compute_transmission_mode,
    normalize_calls,
    normalize_monosomy,
    parental_origin,
    process_occurrence,
)
from radiant.tasks.vcf.vcf_utils import (
    ZYGOSITY_HET,
    ZYGOSITY_HOM,
    ZYGOSITY_UNK,
    ZYGOSITY_WT,
)

from .vcf_test_utils import variant

task = RadiantGermlineAnnotationTask(
    task_id=1,
    part=1,
    analysis_type="germline",
    experiments=[
        Experiment(
            seq_id=1,
            patient_id=1,
            aliquot="SA0001",
            family_role="proband",
            affected_status="affected",
            sex="F",
            experimental_strategy="wgs",
            request_priority="routine",
        )
    ],
    vcf_filepath="",
)
common = Common(task.task_id, task.part, "1-1000-AC-A", "hash", "1", 1000, 1000, "AC", "A")


def test_one_sample():
    v = variant("test_occurrence_one_sample.vcf")
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_homozygous():
    v = variant("test_occurrence_zygosity.vcf")
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_heterozygous():
    v = variant("test_occurrence_zygosity.vcf", 2)
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_unknown():
    v = variant("test_occurrence_zygosity.vcf", 3)
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_wild_type():
    v = variant("test_occurrence_zygosity.vcf", 4)
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_hemizygous():
    v = variant("test_occurrence_zygosity.vcf", 5)
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_filter_pass():
    v = variant("test_occurrence_filter.vcf", 1)
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_filter_empty():
    v = variant("test_occurrence_filter.vcf", 2)
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_filter_defined():
    v = variant("test_occurrence_filter.vcf", 3)
    occ = process_occurrence(v, Pedigree(task, ["SA0001"]), common).get(1, None)
    assert occ is not None


def test_multi_sample():
    multi_sample_task = RadiantGermlineAnnotationTask(
        task_id=1,
        part=1,
        analysis_type="germline",
        experiments=[
            Experiment(
                seq_id=1,
                patient_id=1,
                aliquot="SA0001",
                family_role="proband",
                affected_status="affected",
                sex="F",
                experimental_strategy="wgs",
                request_priority="routine",
            ),
            Experiment(
                seq_id=2,
                patient_id=2,
                aliquot="SA0002",
                family_role="mother",
                affected_status="affected",
                sex="F",
                experimental_strategy="wgs",
                request_priority="routine",
            ),
            Experiment(
                seq_id=3,
                patient_id=3,
                aliquot="SA0003",
                family_role="father",
                affected_status="affected",
                sex="M",
                experimental_strategy="wgs",
                request_priority="routine",
            ),
        ],
        vcf_filepath="",
    )
    v = variant("test_occurrence_multi_sample.vcf", 1)
    occ = process_occurrence(v, Pedigree(multi_sample_task, ["SA0001", "SA0002", "SA0003"]), common)

    assert occ.get(1, None) is not None
    assert occ[1]["zygosity"] == "HOM"
    assert occ[1]["aliquot"] == "SA0001"
    assert occ[1]["ad_total"] == 10
    assert occ[1]["ad_ref"] == 3
    assert occ[1]["ad_alt"] == 7
    assert occ.get(2, None) is not None
    assert occ[2]["zygosity"] == "HET"
    assert occ[2]["aliquot"] == "SA0002"
    assert occ[2]["ad_total"] == 30
    assert occ[2]["ad_ref"] == 10
    assert occ[2]["ad_alt"] == 20
    assert occ.get(3, None) is not None
    assert occ[3]["zygosity"] == "HET"
    assert occ[3]["aliquot"] == "SA0003"
    assert occ[3]["ad_total"] == 50
    assert occ[3]["ad_ref"] == 20
    assert occ[3]["ad_alt"] == 30


def test_adjust_zygosity_and_calls():
    assert adjust_calls_and_zygosity([0, 0], ZYGOSITY_WT, 2, 7) == ([-1, -1], "UNK")
    assert adjust_calls_and_zygosity([0], ZYGOSITY_WT, 2, 7) == ([-1], "UNK")
    assert adjust_calls_and_zygosity([0, 1], ZYGOSITY_HET, 7, 2) == ([-1, -1], "UNK")
    assert adjust_calls_and_zygosity([1, 1], ZYGOSITY_HOM, 7, 2) == ([-1, -1], "UNK")
    assert adjust_calls_and_zygosity([-1, -1], ZYGOSITY_UNK, 7, 2) == ([-1, -1], "UNK")
    assert adjust_calls_and_zygosity([0, 0], ZYGOSITY_WT, 7, 2) == ([0, 0], "WT")
    assert adjust_calls_and_zygosity([0], ZYGOSITY_WT, 7, 2) == ([0], "WT")
    assert adjust_calls_and_zygosity([0, 1], ZYGOSITY_HET, 2, 7) == ([0, 1], "HET")
    assert adjust_calls_and_zygosity([1, 1], ZYGOSITY_HOM, 2, 7) == ([1, 1], "HOM")
    assert adjust_calls_and_zygosity([1], ZYGOSITY_HOM, 2, 7) == ([1], "HEM")


def test_normalize_monosomy():
    assert normalize_monosomy((1,)) == (1, 1)
    assert normalize_monosomy((-1,)) == (-1, -1)
    assert normalize_monosomy((0,)) == (0, 0)
    assert normalize_monosomy((0, 1)) == (0, 1)


def test_normalize_calls():
    assert normalize_calls([0, 1]) == (0, 1)
    assert normalize_calls([1, 0]) == (0, 1)
    assert normalize_calls([0, 1]) == (0, 1)
    assert normalize_calls([0, -1]) == (-1, 0)
    assert normalize_calls([1, 0]) == (0, 1)
    assert normalize_calls([1]) == (1,)


def approx_equal_occ(actual: dict, expected: dict, float_tol=1e-6):
    for k, v in expected.items():
        if isinstance(v, float):
            assert math.isclose(actual.get(k), v, rel_tol=float_tol), f"{k} mismatch: {actual.get(k)} != {v}"
        else:
            assert actual.get(k) == v, f"{k} mismatch: {actual.get(k)} != {v}"


@pytest.mark.parametrize("gt_data,expected", list(AUTOSOMAL_ORIGINS_LOOKUP.items()))
def test_parental_origin_autosomal(gt_data, expected):
    child_gt, mother_gt, father_gt = gt_data
    result = parental_origin("1", child_gt, mother_gt, father_gt)  # autosomal example
    assert result == expected, f"Autosomal failed for {gt_data}: expected {expected}, got {result}"


@pytest.mark.parametrize("gt_data,expected", list(X_ORIGINS_LOOKUP.items()))
def test_parental_origin_x(gt_data, expected):
    child_gt, mother_gt, father_gt = gt_data
    result = parental_origin("X", child_gt, mother_gt, father_gt)
    assert result == expected, f"X-linked failed for {gt_data}: expected {expected}, got {result}"


@pytest.mark.parametrize("gt_data,expected", list(Y_ORIGINS_LOOKUP.items()))
def test_parental_origin_y(gt_data, expected):
    child_gt, mother_gt, father_gt = gt_data
    result = parental_origin("Y", child_gt, mother_gt, father_gt)
    assert result == expected, f"Y-linked failed for {gt_data}: expected {expected}, got {result}"


@pytest.mark.parametrize(
    "inputs,expected",
    [
        (
            ("1", "Male", (0, 1), (0, 0), (0, 0), False, False),
            "autosomal_dominant_de_novo",
        ),
        (("1", "Male", (0, 1), (0, 0), (0, 1), False, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (0, 0), (1, 1), False, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (0, 1), (0, 0), True, False), "autosomal_dominant"),
        (("1", "Male", (0, 1), (0, 1), (1, 1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (0, 1), (0, 1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (0, 1), (-1, -1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (0, 1), (-1, -1), True, False), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (0, 0), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (0, 0), True, False), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (0, 1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (0, 1), True, False), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (1, 1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (1, 1), True, False), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (-1, -1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (1, 1), (-1, -1), True, False), "autosomal_dominant"),
        (("1", "Male", (0, 1), (-1, -1), (0, 1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (-1, -1), (0, 1), False, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (-1, -1), (1, 1), True, True), "autosomal_dominant"),
        (("1", "Male", (0, 1), (-1, -1), (1, 1), False, True), "autosomal_dominant"),
        (("1", "Male", (1, 1), (0, 1), (0, 1), False, False), "autosomal_recessive"),
        (("1", "Male", (1, 1), (0, 1), (1, 1), False, True), "autosomal_recessive"),
        (("1", "Male", (1, 1), (1, 1), (0, 1), True, False), "autosomal_recessive"),
        (("1", "Male", (1, 1), (1, 1), (1, 1), True, True), "autosomal_recessive"),
    ],
)
def test_autosomal_transmission_mode(inputs, expected):
    assert compute_transmission_mode(*inputs) == expected


@pytest.mark.parametrize(
    "inputs,expected",
    [
        (
            ("X", "Female", (0, 1), (0, 0), (0, 0), False, False),
            "x_linked_dominant_de_novo",
        ),
        (
            ("X", "Male", (1, 1), (0, 0), (0, 0), False, False),
            "x_linked_recessive_de_novo",
        ),
        (("X", "Female", (0, 1), (0, 0), (0, 1), False, True), "x_linked_dominant"),
        (("X", "Male", (1, 1), (0, 0), (0, 1), False, False), "x_linked_recessive"),
        (("X", "Male", (1,), (0, 0), (0, 1), False, False), "x_linked_recessive"),
        (("X", "Male", (1, 1), (0, 0), (1, 1), False, True), "x_linked_recessive"),
        (("X", "Female", (0, 1), (1, 1), (0, 0), True, False), "x_linked_dominant"),
        (("X", "Male", (1, 1), (1, 1), (0, 0), True, False), "x_linked_recessive"),
        (("X", "Female", (0, 1), (1, 1), (0, 1), True, True), "x_linked_dominant"),
        (("X", "Male", (1, 1), (1, 1), (0, 1), True, False), "x_linked_recessive"),
        (("X", "Male", (0, 1), (1, 1), (0, 1), True, True), "x_linked_recessive"),
        (("X", "Male", (1, 1), (1, 1), (1, 1), True, True), "x_linked_recessive"),
        (("X", "Female", (0, 1), (-1, -1), (0, 1), False, True), "x_linked_dominant"),
        (("X", "Female", (0, 1), (-1, -1), (0, 1), True, True), "x_linked_dominant"),
        (("X", "Male", (1, 1), (-1, -1), (0, 1), True, False), "x_linked_recessive"),
        (("X", "Male", (1, 1), (-1, -1), (0, 1), False, False), "x_linked_recessive"),
        (("X", "Male", (1, 1), (-1, -1), (1, 1), False, True), "x_linked_recessive"),
        (("X", "Male", (1, 1), (-1, -1), (1, 1), True, True), "x_linked_recessive"),
        (("X", "Female", (1, 1), (1, 1), (0, 0), True, False), "x_linked_recessive"),
        (("X", "Female", (1, 1), (1, 1), (0, 1), True, False), "x_linked_recessive"),
        (("X", "Female", (1, 1), (1, 1), (1, 1), True, True), "x_linked_recessive"),
        (("X", "Female", (1, 1), (1, 1), (-1, -1), True, True), "x_linked_dominant"),
        (("X", "Female", (1, 1), (1, 1), (-1, -1), True, False), "x_linked_dominant"),
    ],
)
def test_sexual_transmission_mode(inputs, expected):
    assert compute_transmission_mode(*inputs) == expected


@pytest.mark.parametrize(
    "inputs,expected",
    [
        (("1", "Male", None, None, None, False, False), "unknown_parents_genotype"),
        (("1", "Male", (0, 1), None, (0, 1), False, False), "unknown_father_genotype"),
        (("1", "Male", (0, 1), (0, 1), None, False, False), "unknown_mother_genotype"),
        (("1", "Male", (0, 0), (0, 1), (0, 1), False, False), "non_carrier_proband"),
        (
            ("1", "Male", (-1, -1), (0, 1), (0, 1), False, False),
            "unknown_proband_genotype",
        ),
    ],
)
def test_transmission_mode_edge_cases(inputs, expected):
    assert compute_transmission_mode(*inputs) == expected
