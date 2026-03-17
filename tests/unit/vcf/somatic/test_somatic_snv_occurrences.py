import pytest

from radiant.tasks.vcf.snv.somatic.occurrence import adjust_somatic_calls_and_zygosity
from radiant.tasks.vcf.vcf_utils import ZYGOSITY, ZYGOSITY_HET, ZYGOSITY_HOM, ZYGOSITY_WT


@pytest.mark.parametrize(
    "calls,zygosity",
    [
        ([0, 0], ZYGOSITY_WT),  # standard ref-only
        ([0], ZYGOSITY_WT),  # single ref call
        ([], ZYGOSITY_WT),  # empty calls
        ([], ZYGOSITY_HOM),  # high ad_alt irrelevant when no 1 in calls
    ],
)
def test_no_alt_returns_wt(calls, zygosity):
    result_calls, result_zygosity = adjust_somatic_calls_and_zygosity(calls, zygosity, ad_alt=None)
    assert result_calls == calls
    assert result_zygosity == ZYGOSITY[ZYGOSITY_WT]


@pytest.mark.parametrize(
    "calls,ad_alt",
    [
        ([0, 1], None),  # no depth info
        ([0, 1], 0),  # zero reads
        ([0, 1], 1),  # below threshold
    ],
)
def test_alt_insufficient_depth_returns_unk(calls, ad_alt):
    result_calls, result_zygosity = adjust_somatic_calls_and_zygosity(calls, ZYGOSITY_HET, ad_alt=ad_alt)
    assert result_calls == [-1] * len(calls)
    assert result_zygosity == "UNK"


@pytest.mark.parametrize("ad_alt", [2, 5, 100])
def test_single_alt_call_sufficient_depth_returns_hem(ad_alt):
    result_calls, result_zygosity = adjust_somatic_calls_and_zygosity([1], ZYGOSITY_HOM, ad_alt=ad_alt)
    assert result_calls == [1]
    assert result_zygosity == "HEM"


@pytest.mark.parametrize(
    "calls,zygosity,ad_alt",
    [
        ([0, 1], ZYGOSITY_HET, 2),  # exact threshold
        ([0, 1], ZYGOSITY_HET, 10),  # standard HET
        ([1, 1], ZYGOSITY_HOM, 30),  # HOM
    ],
)
def test_multi_call_sufficient_depth_returns_zygosity_label(calls, zygosity, ad_alt):
    result_calls, result_zygosity = adjust_somatic_calls_and_zygosity(calls, zygosity, ad_alt=ad_alt)
    assert result_calls == calls
    assert result_zygosity == ZYGOSITY[zygosity]


@pytest.mark.parametrize(
    "calls,zygosity,ad_alt",
    [
        ([0, 0], ZYGOSITY_WT, None),  # WT path
        ([0, 1], ZYGOSITY_HET, 1),  # UNK path
        ([1], ZYGOSITY_HOM, 5),  # HEM path
        ([0, 1], ZYGOSITY_HET, 10),  # zygosity label path
    ],
)
def test_return_type_is_always_tuple_of_list_and_str(calls, zygosity, ad_alt):
    result = adjust_somatic_calls_and_zygosity(calls, zygosity, ad_alt=ad_alt)
    assert isinstance(result, tuple) and len(result) == 2
    assert isinstance(result[0], list)
    assert isinstance(result[1], str)


@pytest.mark.parametrize(
    "calls,zygosity,ad_alt",
    [
        ([0, 1], ZYGOSITY_HET, 5),
        ([1, 1], ZYGOSITY_HOM, 20),
    ],
)
def test_input_calls_list_is_not_mutated(calls, zygosity, ad_alt):
    original = list(calls)
    adjust_somatic_calls_and_zygosity(calls, zygosity, ad_alt=ad_alt)
    assert calls == original
