from unittest.mock import MagicMock

import pytest

from radiant.tasks.vcf.experiment import Experiment
from radiant.tasks.vcf.snv.somatic.process import get_somatic_indexes


def make_experiment(aliquot: str, histology_type: str) -> Experiment:
    exp = MagicMock(spec=Experiment)
    exp.aliquot = aliquot
    exp.histology_type = histology_type
    return exp


def test_tumor_first_normal_second():
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    samples = ["SAMPLE_T", "SAMPLE_N"]
    tumor_index, normal_index = get_somatic_indexes(experiments, samples)
    assert tumor_index == 0
    assert normal_index == 1


def test_normal_first_tumor_second():
    """Column order in VCF may differ from experiment list order."""
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    samples = ["SAMPLE_N", "SAMPLE_T"]  # VCF has normal first
    tumor_index, normal_index = get_somatic_indexes(experiments, samples)
    assert tumor_index == 1
    assert normal_index == 0


def test_experiment_list_order_does_not_affect_result():
    """Indexes should reflect VCF column positions, not experiment list order."""
    experiments_a = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    experiments_b = [
        make_experiment("SAMPLE_N", "normal"),
        make_experiment("SAMPLE_T", "tumoral"),
    ]
    samples = ["SAMPLE_N", "SAMPLE_T"]
    assert get_somatic_indexes(experiments_a, samples) == get_somatic_indexes(experiments_b, samples)


def test_missing_tumor_raises():
    experiments = [make_experiment("SAMPLE_N", "normal")]
    with pytest.raises(ValueError, match="tumor"):
        get_somatic_indexes(experiments, ["SAMPLE_N"])


def test_missing_normal_raises():
    experiments = [make_experiment("SAMPLE_T", "tumoral")]
    with pytest.raises(ValueError, match="normal"):
        get_somatic_indexes(experiments, ["SAMPLE_T"])


def test_empty_experiments_raises():
    with pytest.raises(ValueError):
        get_somatic_indexes([], ["SAMPLE_T", "SAMPLE_N"])


def test_aliquot_not_in_samples_raises():
    """Experiment aliquot that isn't present in VCF samples should raise."""
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    with pytest.raises(ValueError):
        get_somatic_indexes(experiments, ["SAMPLE_T", "SAMPLE_WRONG"])


def test_unknown_histology_type_ignored():
    """Extra experiments with unrecognised histology_type should not affect result."""
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
        make_experiment("SAMPLE_X", "metastasis"),  # unknown type
    ]
    samples = ["SAMPLE_N", "SAMPLE_T", "SAMPLE_X"]
    tumor_index, normal_index = get_somatic_indexes(experiments, samples)
    assert tumor_index == 1
    assert normal_index == 0


def test_returns_tuple_of_two_ints():
    experiments = [
        make_experiment("SAMPLE_T", "tumoral"),
        make_experiment("SAMPLE_N", "normal"),
    ]
    result = get_somatic_indexes(experiments, ["SAMPLE_N", "SAMPLE_T"])
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(i, int) for i in result)
