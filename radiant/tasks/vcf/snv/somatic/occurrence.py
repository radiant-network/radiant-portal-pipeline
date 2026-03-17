from cyvcf2 import Variant
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, NestedField, StringType

from radiant.tasks.iceberg.utils import merge_schemas
from radiant.tasks.vcf.experiment import Experiment
from radiant.tasks.vcf.snv.common import SCHEMA as COMMON_SCHEMA
from radiant.tasks.vcf.snv.common import Common
from radiant.tasks.vcf.vcf_utils import ZYGOSITY, ZYGOSITY_HET, ZYGOSITY_HOM, ZYGOSITY_WT, calls_without_phased

SCHEMA = merge_schemas(
    COMMON_SCHEMA,
    Schema(
        NestedField(500, "part", IntegerType(), required=True),
        NestedField(508, "quality", FloatType(), required=False),
        NestedField(509, "filter", StringType(), required=False),
        NestedField(510, "info_hotspotallele", StringType(), required=False),
        NestedField(511, "info_old_record", StringType(), required=False),
        NestedField(512, "info_baseq_rank_sum", FloatType(), required=False),
        NestedField(513, "info_excess_het", FloatType(), required=False),
        NestedField(514, "info_fs", FloatType(), required=False),
        NestedField(515, "info_ds", BooleanType(), required=False),
        NestedField(516, "info_fraction_informative_reads", FloatType(), required=False),
        NestedField(517, "info_inbreed_coeff", FloatType(), required=False),
        NestedField(518, "info_mleac", IntegerType(), required=False),
        NestedField(519, "info_mleaf", FloatType(), required=False),
        NestedField(520, "info_mq", FloatType(), required=False),
        NestedField(521, "info_mq0", FloatType(), required=False),
        NestedField(522, "info_m_qrank_sum", FloatType(), required=False),
        NestedField(523, "info_qd", FloatType(), required=False),
        NestedField(524, "info_r2_5p_bias", FloatType(), required=False),
        NestedField(525, "info_read_pos_rank_sum", FloatType(), required=False),
        NestedField(526, "info_sor", FloatType(), required=False),
        NestedField(527, "info_vqslod", FloatType(), required=False),
        NestedField(528, "info_culprit", StringType(), required=False),
        NestedField(529, "info_dp", IntegerType(), required=False),
        NestedField(530, "info_haplotype_score", FloatType(), required=False),
        NestedField(540, "tumor_seq_id", IntegerType(), required=True),
        NestedField(541, "tumor_calls", ListType(241, IntegerType()), required=False),
        NestedField(542, "tumor_dp", IntegerType(), required=False),
        NestedField(543, "tumor_gq", IntegerType(), required=False),
        NestedField(544, "tumor_has_alt", BooleanType(), required=False),
        NestedField(545, "tumor_af", FloatType(), required=False),
        NestedField(546, "tumor_zygosity", StringType(), required=False),
        NestedField(547, "tumor_ad_ref", IntegerType(), required=False),
        NestedField(548, "tumor_ad_alt", IntegerType(), required=False),
        NestedField(549, "tumor_ad_total", IntegerType(), required=False),
        NestedField(550, "tumor_ad_ratio", FloatType(), required=False),
        NestedField(551, "tumor_phased", BooleanType(), required=False),
        NestedField(552, "tumor_gt_status", StringType(), required=False),
        NestedField(560, "normal_seq_id", IntegerType(), required=False),
        NestedField(561, "normal_calls", ListType(261, IntegerType()), required=False),
        NestedField(562, "normal_dp", IntegerType(), required=False),
        NestedField(563, "normal_gq", IntegerType(), required=False),
        NestedField(564, "normal_has_alt", BooleanType(), required=False),
        NestedField(565, "normal_af", FloatType(), required=False),
        NestedField(566, "normal_zygosity", StringType(), required=False),
        NestedField(567, "normal_ad_ref", IntegerType(), required=False),
        NestedField(568, "normal_ad_alt", IntegerType(), required=False),
        NestedField(569, "normal_ad_total", IntegerType(), required=False),
        NestedField(570, "normal_ad_ratio", FloatType(), required=False),
        NestedField(571, "normal_phased", BooleanType(), required=False),
        NestedField(572, "normal_gt_status", StringType(), required=False),
    ),
)


def process_occurrence(
    record: Variant, experiments: list[Experiment], common: Common, tumor_index: int, normal_index: int
) -> dict:
    """


    :param record:
    :param experiments:
    :param common:
    :param tumor_index:
    :param normal_index:
    :return:
    """
    occurrences = {}

    info_fields = record.INFO
    quality = int(record.QUAL) if record.QUAL is not None else None
    filter = record.FILTER or "PASS"
    # hotspotallele = info_fields.get("HotSpotAllele", None)
    old_record = info_fields.get("OLD_RECORD", None)
    baseq_ranksum = info_fields.get("BaseQRankSum", None)
    fs = info_fields.get("FS", None)
    ds = info_fields.get("DS", None)
    fraction_informative_reads = info_fields.get("FractionInformativeReads", None)
    inbreed_coeff = info_fields.get("InbreedCoeff", None)
    mleac = info_fields.get("MLEAC", None)
    mleaf = info_fields.get("MLEAF", None)
    mq = info_fields.get("MQ", None)
    # mq0 = info_fields.get("MQ0", None)
    mq_ranksum = info_fields.get("MQRankSum", None)
    qd = info_fields.get("QD", None)
    r2_5p_bias = info_fields.get("R2_5P_bias", None)
    read_pos_rank_su = info_fields.get("ReadPosRankSum", None)
    sor = info_fields.get("SOR", None)
    vqslod = info_fields.get("VQSLod", None)
    culprit = info_fields.get("Culprit", None)
    info_dp = info_fields.get("DP", None)
    haplotype_score = info_fields.get("HaplotypeScore", None)
    excess_het = info_fields.get("ExcessHet", None)

    tumor_exp = experiments[tumor_index]
    normal_exp = experiments[normal_index]

    # Tumor FORMAT
    t_dp = record.format("DP")[tumor_index][0] if "DP" in record.FORMAT else 0

    # FIXME `GQ` doesn't exist for somatics,
    #        need to check with bioinformatics team on what the best quality metric for calls
    t_gq = record.format("GQ")[tumor_index][0] if "GQ" in record.FORMAT else 0
    t_ad_ref = record.gt_ref_depths[tumor_index] if record.gt_ref_depths[tumor_index] > 0 else None
    t_ad_alt = record.gt_alt_depths[tumor_index] if record.gt_alt_depths[tumor_index] > 0 else None
    t_calls = calls_without_phased(record, tumor_index)
    t_calls, t_zygosity = adjust_somatic_calls_and_zygosity(t_calls, record.gt_types[tumor_index], t_ad_ref, t_ad_alt)
    t_has_alt = 1 in t_calls
    t_ad_total = record.gt_depths[tumor_index] if record.gt_depths[tumor_index] > 0 else None
    t_ad_ratio = record.gt_alt_freqs[tumor_index] if record.gt_alt_freqs[tumor_index] > 0 else None
    t_af = t_ad_ratio
    t_phased = record.gt_phases[tumor_index]

    # Normal FORMAT
    n_dp = record.format("DP")[normal_index][0] if "DP" in record.FORMAT else 0

    # FIXME same thing here for `GQ`
    n_gq = record.format("GQ")[normal_index][0] if "GQ" in record.FORMAT else 0
    n_ad_ref = record.gt_ref_depths[normal_index] if record.gt_ref_depths[normal_index] > 0 else None
    n_ad_alt = record.gt_alt_depths[normal_index] if record.gt_alt_depths[normal_index] > 0 else None
    n_calls = calls_without_phased(record, normal_index)
    n_calls, n_zyg = adjust_somatic_calls_and_zygosity(n_calls, record.gt_types[normal_index], n_ad_ref, n_ad_alt)
    n_has_alt = 1 in n_calls if n_calls is not None else None
    n_ad_total = record.gt_depths[normal_index] if record.gt_depths[normal_index] > 0 else None
    n_ad_ratio = record.gt_alt_freqs[normal_index] if record.gt_alt_freqs[normal_index] > 0 else None
    n_af = n_ad_ratio
    n_phased = record.gt_phases[normal_index]

    occurrences[tumor_exp.seq_id] = {
        # common
        "part": common.part,
        "task_id": common.task_id,
        "locus": common.locus,
        "locus_hash": common.locus_hash,
        "chromosome": common.chromosome,
        "start": common.start,
        "end": common.end,
        "reference": common.reference,
        "alternate": common.alternate,
        # info
        "quality": quality,
        "filter": filter,
        "info_old_record": old_record,
        "info_baseq_rank_sum": baseq_ranksum,
        "info_excess_het": excess_het,
        "info_fs": fs,
        "info_ds": ds,
        "info_fraction_informative_reads": fraction_informative_reads,
        "info_inbreed_coeff": inbreed_coeff,
        "info_mleac": mleac,
        "info_mleaf": mleaf,
        "info_mq": mq,
        "info_mq0": info_fields.get("MQ0", None),
        "info_m_qrank_sum": mq_ranksum,
        "info_qd": qd,
        "info_r2_5p_bias": r2_5p_bias,
        "info_read_pos_rank_sum": read_pos_rank_su,
        "info_sor": sor,
        "info_vqslod": vqslod,
        "info_culprit": culprit,
        "info_dp": info_dp,
        "info_haplotype_score": haplotype_score,
        "info_hotspotallele": info_fields.get("HotspotAllele", None),
        "info_cal": info_fields.get("CAL", None),
        # tumor FORMAT
        "tumor_seq_id": tumor_exp.seq_id,
        "tumor_calls": t_calls,
        "tumor_dp": t_dp if t_dp > 0 else None,
        "tumor_gq": t_gq if t_gq > 0 else None,
        "tumor_ad_ref": t_ad_ref,
        "tumor_ad_alt": t_ad_alt,
        "tumor_ad_total": t_ad_total,
        "tumor_ad_ratio": t_ad_ratio,
        "tumor_af": t_af,
        "tumor_zygosity": t_zygosity,
        "tumor_phased": t_phased,
        "tumor_has_alt": t_has_alt,
        "tumor_gt_status": None,
        # normal FORMAT
        "normal_seq_id": normal_exp.seq_id,
        "normal_calls": n_calls,
        "normal_dp": n_dp if n_dp > 0 else None,
        "normal_gq": n_gq if n_gq > 0 else None,
        "normal_ad_ref": n_ad_ref,
        "normal_ad_alt": n_ad_alt,
        "normal_ad_total": n_ad_total,
        "normal_ad_ratio": n_ad_ratio,
        "normal_af": n_af,
        "normal_zygosity": n_zyg,
        "normal_phased": n_phased,
        "normal_has_alt": n_has_alt,
        "normal_gt_status": None,
    }

    return occurrences


def adjust_somatic_calls_and_zygosity(
    calls: list[int], zygosity: int, ad_alt: int | None
) -> tuple[list[int], str]:
    """
    Adjusts the somatic calls and zygosity based on alternate allele depths.

    Parameters:
        calls (list[int]): A list of genotype calls, where each call represents an allele
        (e.g., 0 for reference, 1 for alternate).
        zygosity (int): The zygosity type, represented as an integer (e.g., 0 for WT, 1 for HET, 3 for HOM, 2 for UNK).
        ad_alt (Optional[int]): The depth of reads supporting the alternate allele. Can be None if not available.

    Returns:
    Tuple[list[int], str]: A tuple containing:
        - The adjusted list of genotype calls.
        - The zygosity as a string (e.g., "WT", "HET", "HOM", "UNK", or "HEM").

    Behavior:

            For somatic variants, zygosity categories (HET/HOM/WT) from germline calling
            are not meaningful. Instead, we determine presence based on allelic depth (AD)
            support:

                       alt allele present (1 in calls)?
                       /                    \
                     YES                     NO
                      |                       |
                ad_alt >= 2?            return calls,
                /          \               ZYGOSITY_WT ("WT")
              YES            NO
               |              |
            return calls,  return -1s,
            "HEM" if        "UNK"
            single call,
            else ZYGOSITY[z]

    """
    has_alt = 1 in calls if calls else False

    if not has_alt:
        # No alt allele called — treat as wild-type
        return calls, ZYGOSITY[ZYGOSITY_WT]

    # Alt allele present but insufficient read support — unknown
    if ad_alt is None or ad_alt < 2:
        return [-1 for _ in range(len(calls))], "UNK"

    # Single-call with alt — hemizygous (e.g. LOH or haploid region)
    if len(calls) == 1:
        return calls, "HEM"

    return calls, ZYGOSITY[zygosity]
