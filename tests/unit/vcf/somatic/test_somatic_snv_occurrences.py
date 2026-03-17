from unittest.mock import MagicMock, patch

import pytest

from radiant.tasks.vcf.snv.somatic.occurrence import adjust_somatic_calls_and_zygosity, process_occurrence
from radiant.tasks.vcf.vcf_utils import ZYGOSITY, ZYGOSITY_HET, ZYGOSITY_HOM, ZYGOSITY_WT


def make_common(
    part=1,
    task_id="task_1",
    locus="chr1:100",
    locus_hash="abc123",
    chromosome="chr1",
    start=100,
    end=101,
    reference="A",
    alternate="T",
):
    common = MagicMock()
    common.part = part
    common.task_id = task_id
    common.locus = locus
    common.locus_hash = locus_hash
    common.chromosome = chromosome
    common.start = start
    common.end = end
    common.reference = reference
    common.alternate = alternate
    return common


def make_experiment(seq_id):
    exp = MagicMock()
    exp.seq_id = seq_id
    return exp


def make_record(
    qual=50.0,
    filter_val=None,
    info=None,
    format_keys=("DP", "GQ"),
    dp_values=(100, 80),
    gq_values=(30, 25),
    gt_ref_depths=(10, 20),
    gt_alt_depths=(90, 5),
    gt_depths=(100, 25),
    gt_alt_freqs=(0.9, 0.2),
    gt_types=(1, 0),
    gt_phases=(False, False),
):
    record = MagicMock()
    record.QUAL = qual
    record.FILTER = filter_val
    record.INFO = info or {}
    record.FORMAT = format_keys

    record.format.side_effect = lambda key: (
        [[dp_values[0]], [dp_values[1]]] if key == "DP" else [[gq_values[0]], [gq_values[1]]] if key == "GQ" else None
    )

    record.gt_ref_depths = gt_ref_depths
    record.gt_alt_depths = gt_alt_depths
    record.gt_depths = gt_depths
    record.gt_alt_freqs = gt_alt_freqs
    record.gt_types = gt_types
    record.gt_phases = gt_phases

    return record


TUMOR_SEQ_ID = 10
NORMAL_SEQ_ID = 20
TUMOR_INDEX = 0
NORMAL_INDEX = 1


@pytest.fixture
def experiments():
    return [make_experiment(TUMOR_SEQ_ID), make_experiment(NORMAL_SEQ_ID)]


@pytest.fixture
def common():
    return make_common()


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


@pytest.mark.parametrize(
    "calls,zygosity,ad_alt,expected_calls,expected_zyg",
    [
        # No alt → WT
        ([0, 0], 0, None, [0, 0], ZYGOSITY[ZYGOSITY_WT]),
        ([0, 0], 0, 5, [0, 0], ZYGOSITY[ZYGOSITY_WT]),
        # Alt present, insufficient depth → UNK
        ([0, 1], 1, None, [-1, -1], "UNK"),
        ([0, 1], 1, 1, [-1, -1], "UNK"),
        # Alt present, single call → HEM
        ([1], 1, 3, [1], "HEM"),
        # Alt present, multi-call, sufficient depth → ZYGOSITY lookup
        ([0, 1], 1, 5, [0, 1], ZYGOSITY[1]),
        ([1, 1], 3, 10, [1, 1], ZYGOSITY[3]),
    ],
)
def test_adjust_somatic_calls_and_zygosity(calls, zygosity, ad_alt, expected_calls, expected_zyg):
    result_calls, result_zyg = adjust_somatic_calls_and_zygosity(calls, zygosity, ad_alt)
    assert result_calls == expected_calls
    assert result_zyg == expected_zyg


def run_process(record, experiments, common, tumor_index=TUMOR_INDEX, normal_index=NORMAL_INDEX):
    with (
        patch("radiant.tasks.vcf.vcf_utils.calls_without_phased") as mock_calls,
        patch("radiant.tasks.vcf.snv.somatic.occurrence.adjust_somatic_calls_and_zygosity") as mock_adjust,
    ):
        mock_calls.side_effect = lambda r, idx: [0, 1] if idx == tumor_index else [0, 0]
        mock_adjust.side_effect = lambda calls, zyg, ad_alt: (calls, "HET") if 1 in calls else (calls, "WT")
        return process_occurrence(record, experiments, common, tumor_index, normal_index)


def test_returns_dict_keyed_by_tumor_seq_id(experiments, common):
    record = make_record()
    result = run_process(record, experiments, common)
    assert TUMOR_SEQ_ID in result
    assert len(result) == 1


def test_common_fields_are_mapped(experiments, common):
    record = make_record()
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["part"] == common.part
    assert result["task_id"] == common.task_id
    assert result["locus"] == common.locus
    assert result["locus_hash"] == common.locus_hash
    assert result["chromosome"] == common.chromosome
    assert result["start"] == common.start
    assert result["end"] == common.end
    assert result["reference"] == common.reference
    assert result["alternate"] == common.alternate


def test_seq_ids_are_set(experiments, common):
    record = make_record()
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_seq_id"] == TUMOR_SEQ_ID
    assert result["normal_seq_id"] == NORMAL_SEQ_ID


@pytest.mark.parametrize(
    "qual,expected",
    [
        (50.0, 50),
        (0.0, 0),
        (None, None),
    ],
)
def test_quality_parsing(qual, expected, experiments, common):
    record = make_record(qual=qual)
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["quality"] == expected


@pytest.mark.parametrize(
    "filter_val,expected",
    [
        (None, "PASS"),
        ("LowQual", "LowQual"),
        ("PASS", "PASS"),
    ],
)
def test_filter_parsing(filter_val, expected, experiments, common):
    record = make_record(filter_val=filter_val)
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["filter"] == expected


@pytest.mark.parametrize(
    "info_key,result_key,value",
    [
        ("OLD_RECORD", "info_old_record", "old_val"),
        ("BaseQRankSum", "info_baseq_rank_sum", 1.5),
        ("ExcessHet", "info_excess_het", 0.01),
        ("FS", "info_fs", 2.3),
        ("DS", "info_ds", True),
        ("FractionInformativeReads", "info_fraction_informative_reads", 0.95),
        ("InbreedCoeff", "info_inbreed_coeff", -0.1),
        ("MLEAC", "info_mleac", 2),
        ("MLEAF", "info_mleaf", 0.5),
        ("MQ", "info_mq", 60.0),
        ("MQ0", "info_mq0", 0.0),
        ("MQRankSum", "info_m_qrank_sum", -1.2),
        ("QD", "info_qd", 25.0),
        ("R2_5P_bias", "info_r2_5p_bias", 0.3),
        ("ReadPosRankSum", "info_read_pos_rank_sum", 0.7),
        ("SOR", "info_sor", 0.8),
        ("VQSLod", "info_vqslod", 10.5),
        ("Culprit", "info_culprit", "MQ"),
        ("DP", "info_dp", 150),
        ("HaplotypeScore", "info_haplotype_score", 3.2),
        ("HotspotAllele", "info_hotspotallele", "TP53"),
        ("CAL", "info_cal", "COSMIC"),
    ],
)
def test_info_fields_are_mapped(info_key, result_key, value, experiments, common):
    record = make_record(info={info_key: value})
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result[result_key] == value


@pytest.mark.parametrize(
    "result_key",
    [
        "info_old_record",
        "info_baseq_rank_sum",
        "info_excess_het",
        "info_fs",
        "info_ds",
        "info_mq",
        "info_culprit",
        "info_hotspotallele",
        "info_cal",
    ],
)
def test_missing_info_fields_are_none(result_key, experiments, common):
    record = make_record(info={})
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result[result_key] is None


@pytest.mark.parametrize("dp,expected", [(100, 100), (0, None)])
def test_tumor_dp_zero_coalesced(dp, expected, experiments, common):
    record = make_record(dp_values=(dp, 80))
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_dp"] == expected


@pytest.mark.parametrize("dp,expected", [(80, 80), (0, None)])
def test_normal_dp_zero_coalesced(dp, expected, experiments, common):
    record = make_record(dp_values=(100, dp))
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["normal_dp"] == expected


@pytest.mark.parametrize("gq,expected", [(30, 30), (0, None)])
def test_tumor_gq_zero_coalesced(gq, expected, experiments, common):
    record = make_record(gq_values=(gq, 25))
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_gq"] == expected


@pytest.mark.parametrize("ad_ref,expected", [(10, 10), (0, None)])
def test_tumor_ad_ref_zero_coalesced(ad_ref, expected, experiments, common):
    record = make_record(gt_ref_depths=(ad_ref, 20))
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_ad_ref"] == expected


@pytest.mark.parametrize("ad_alt,expected", [(90, 90), (0, None)])
def test_tumor_ad_alt_zero_coalesced(ad_alt, expected, experiments, common):
    record = make_record(gt_alt_depths=(ad_alt, 5))
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_ad_alt"] == expected


@pytest.mark.parametrize("ad_total,expected", [(100, 100), (0, None)])
def test_tumor_ad_total_zero_coalesced(ad_total, expected, experiments, common):
    record = make_record(gt_depths=(ad_total, 25))
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_ad_total"] == expected


@pytest.mark.parametrize("freq,expected", [(0.9, 0.9), (0.0, None)])
def test_tumor_af_zero_coalesced(freq, expected, experiments, common):
    record = make_record(gt_alt_freqs=(freq, 0.2))
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_af"] == expected
    assert result["tumor_ad_ratio"] == expected  # af mirrors ad_ratio


def test_gt_status_fields_are_none(experiments, common):
    record = make_record()
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_gt_status"] is None
    assert result["normal_gt_status"] is None


def test_missing_format_dp_defaults_to_none(experiments, common):
    record = make_record(format_keys=("GQ",))  # no DP key
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    # DP fallback is 0, which is then coalesced to None
    assert result["tumor_dp"] is None
    assert result["normal_dp"] is None


def test_missing_format_gq_defaults_to_none(experiments, common):
    record = make_record(format_keys=("DP",))  # no GQ key
    result = run_process(record, experiments, common)[TUMOR_SEQ_ID]
    assert result["tumor_gq"] is None
    assert result["normal_gq"] is None
