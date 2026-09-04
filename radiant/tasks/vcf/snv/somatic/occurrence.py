from cyvcf2 import Variant
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, NestedField, StringType

from radiant.tasks.iceberg.utils import merge_schemas
from radiant.tasks.vcf.experiment import Experiment
from radiant.tasks.vcf.snv.common import SCHEMA as COMMON_SCHEMA
from radiant.tasks.vcf.snv.common import Common
from radiant.tasks.vcf.vcf_utils import ZYGOSITY, ZYGOSITY_WT, calls_without_phased

SCHEMA = merge_schemas(
    COMMON_SCHEMA,
    Schema(
        NestedField(500, "part", IntegerType(), required=True),
        NestedField(501, "tenant_code", StringType(), required=True),
        NestedField(502, "quality", FloatType(), required=False),
        NestedField(503, "filter", StringType(), required=False),
        NestedField(504, "info_hotspotallele", IntegerType(), required=False),
        NestedField(552, "info_hotspot", BooleanType(), required=False),
        NestedField(505, "info_old_record", StringType(), required=False),
        NestedField(506, "info_baseq_rank_sum", FloatType(), required=False),
        NestedField(507, "info_excess_het", FloatType(), required=False),
        NestedField(508, "info_fs", FloatType(), required=False),
        NestedField(509, "info_ds", BooleanType(), required=False),
        NestedField(510, "info_fraction_informative_reads", FloatType(), required=False),
        NestedField(511, "info_inbreed_coeff", FloatType(), required=False),
        NestedField(512, "info_mleac", IntegerType(), required=False),
        NestedField(513, "info_mleaf", FloatType(), required=False),
        NestedField(514, "info_mq", FloatType(), required=False),
        NestedField(515, "info_mq0", FloatType(), required=False),
        NestedField(516, "info_m_qrank_sum", FloatType(), required=False),
        NestedField(517, "info_qd", FloatType(), required=False),
        NestedField(518, "info_r2_5p_bias", FloatType(), required=False),
        NestedField(519, "info_read_pos_rank_sum", FloatType(), required=False),
        NestedField(520, "info_sor", FloatType(), required=False),
        NestedField(521, "info_vqslod", FloatType(), required=False),
        NestedField(522, "info_culprit", StringType(), required=False),
        NestedField(523, "info_dp", IntegerType(), required=False),
        NestedField(524, "info_haplotype_score", FloatType(), required=False),
        NestedField(525, "info_germq", FloatType(), required=False),
        NestedField(526, "info_tlod", FloatType(), required=False),
        NestedField(527, "info_mapq", FloatType(), required=False),
        NestedField(553, "info_aq", FloatType(), required=False),
        NestedField(528, "tumor_seq_id", IntegerType(), required=True),
        NestedField(529, "tumor_calls", ListType(241, IntegerType()), required=False),
        NestedField(530, "tumor_dp", IntegerType(), required=False),
        NestedField(531, "tumor_has_alt", BooleanType(), required=False),
        NestedField(532, "tumor_af", FloatType(), required=False),
        NestedField(533, "tumor_zygosity", StringType(), required=False),
        NestedField(534, "tumor_ad_ref", IntegerType(), required=False),
        NestedField(535, "tumor_ad_alt", IntegerType(), required=False),
        NestedField(536, "tumor_ad_total", IntegerType(), required=False),
        NestedField(537, "tumor_ad_ratio", FloatType(), required=False),
        NestedField(538, "tumor_phased", BooleanType(), required=False),
        NestedField(539, "tumor_gt_status", StringType(), required=False),
        NestedField(554, "tumor_sq", FloatType(), required=False),
        NestedField(540, "normal_seq_id", IntegerType(), required=False),
        NestedField(541, "normal_calls", ListType(261, IntegerType()), required=False),
        NestedField(542, "normal_dp", IntegerType(), required=False),
        NestedField(543, "normal_has_alt", BooleanType(), required=False),
        NestedField(544, "normal_af", FloatType(), required=False),
        NestedField(545, "normal_zygosity", StringType(), required=False),
        NestedField(546, "normal_ad_ref", IntegerType(), required=False),
        NestedField(547, "normal_ad_alt", IntegerType(), required=False),
        NestedField(548, "normal_ad_total", IntegerType(), required=False),
        NestedField(549, "normal_ad_ratio", FloatType(), required=False),
        NestedField(550, "normal_phased", BooleanType(), required=False),
        NestedField(551, "normal_gt_status", StringType(), required=False),
        NestedField(555, "normal_sq", FloatType(), required=False),
    ),
)


def process_occurrence(
    record: Variant, experiments: list[Experiment], common: Common, tumor_index: int, normal_index: int | None
) -> dict:
    """
    Processes a somatic variant occurrence and extracts relevant information.

    Parameters:
        record (Variant): A `cyvcf2.Variant` object representing the somatic variant to process.
        experiments (list[Experiment]): A list of experiments corresponding to the samples in the VCF, where the tumor sample is at `tumor_index` and the normal sample is at `normal_index`.
        common (Common): A `Common` object containing shared attributes for the variant, such as locus and chromosome.
        tumor_index (int): The index of the tumor sample in the VCF record's samples.
        normal_index (int | None): The index of the normal sample in the VCF record's samples, or `None` for a tumor-only analysis, in which case every `normal_*` value is `None`.

    Returns:
        dict: A dictionary containing the processed occurrence information for the somatic variant, structured as follows:
        {
            tumor_exp.seq_id: {
                "part": ...,
                "task_id": ...,
                "locus": ...,
                "locus_hash": ...,
                "chromosome": ...,
                "start": ...,
                "end": ...,
                "reference": ...,
                "alternate": ...,
                "quality": ...,
                "filter": ...,
                "info_old_record": ...,
                "info_baseq_rank_sum": ...,
                "info_excess_het": ...,
                "info_fs": ...,
                "info_ds": ...,
                "info_fraction_informative_reads": ...,
                "info_inbreed_coeff": ...,
                "info_mleac": ...,
                "info_mleaf": ...,
                "info_mq": ...,
                "info_mq0": ...,
                "info_m_qrank_sum": ...,
                "info_qd": ...,
                "info_r2_5p_bias": ...,
                "info_read_pos_rank_sum": ...,
                "info_sor": ...,
                "info_vqslod": ...,
                "info_culprit": ...,
                "info_dp": ...,
                "info_haplotype_score": ...,
                "info_hotspotallele": ...,
                "info_hotspot": ...,
                "info_cal": ...,
                "info_germq": ...,
                "info_tlod": ...,
                "info_mapq": ...,
                "info_aq": ...,
                "tumor_seq_id": ...,
                "tumor_calls": ...,
                "tumor_dp": ...,
                "tumor_ad_ref": ...,
                "tumor_ad_alt": ...,
                "tumor_ad_total": ...,
                "tumor_ad_ratio": ...,
                "tumor_af": ...,
                "tumor_zygosity": ...,
                "tumor_phased": ...,
                "tumor_has_alt": ...,
                "tumor_gt_status": ...,
                "tumor_sq": ...,
                "normal_seq_id": ...,
                "normal_calls": ...,
                "normal_dp": ...,
                "normal_ad_ref": ...,
                "normal_ad_alt": ...,
                "normal_ad_total": ...,
                "normal_ad_ratio": ...,
                "normal_af": ...,
                "normal_zygosity": ...,
                "normal_phased": ...,
                "normal_has_alt": ...,
                "normal_gt_status": ...,
                "normal_sq": ...,
            }
            ```

    Behavior:
    - Extracts common fields from the `common` object and variant-specific information from the `record`'s INFO and FORMAT fields.
    - For the tumor and normal samples, it processes genotype calls, depth, allele frequencies, and other relevant metrics, adjusting calls and zygosity for somatic context.
    - Returns a dictionary keyed by the tumor sample's `seq_id`, containing all the extracted and processed information for that sample, as well as the normal sample for reference.
    """  # noqa: E501
    occurrences = {}

    info_fields = record.INFO
    info_quality = int(record.QUAL) if record.QUAL is not None else None
    info_filter = record.FILTER or "PASS"
    info_old_record = info_fields.get("OLD_RECORD", None)
    baseq_ranksum = info_fields.get("BaseQRankSum", None)
    info_fs = info_fields.get("FS", None)
    info_ds = info_fields.get("DS", None)
    info_fraction_informative_reads = info_fields.get("FractionInformativeReads", None)
    info_inbreed_coeff = info_fields.get("InbreedCoeff", None)
    info_mleac = info_fields.get("MLEAC", None)
    info_mleaf = info_fields.get("MLEAF", None)
    info_mq = info_fields.get("MQ", None)
    info_mq0 = info_fields.get("MQ0", None)
    info_mq_ranksum = info_fields.get("MQRankSum", None)
    info_qd = info_fields.get("QD", None)
    info_r2_5p_bias = info_fields.get("R2_5P_bias", None)
    info_read_pos_rank_su = info_fields.get("ReadPosRankSum", None)
    info_sor = info_fields.get("SOR", None)
    info_vqslod = info_fields.get("VQSLod", None)
    info_culprit = info_fields.get("Culprit", None)
    info_dp = info_fields.get("DP", None)
    info_haplotype_score = info_fields.get("HaplotypeScore", None)
    info_hotspotallele = info_fields.get("HotspotAllele", None)
    info_hotspot = read_hotspot(info_fields)
    info_cal = info_fields.get("CAL", None)
    info_excess_het = info_fields.get("ExcessHet", None)
    info_germq = info_fields.get("GERMQ", None)
    info_tlod = info_fields.get("TLOD", None)
    info_mapq = info_fields.get("MAPQ", None)
    info_aq = info_fields.get("AQ", None)

    tumor_exp = experiments[tumor_index]
    normal_exp = experiments[normal_index] if normal_index is not None else None

    # Every cyvcf2 `gt_*` property and `format(...)` call re-decodes the FORMAT matrix for
    # *all* samples and allocates a fresh array, so each one is read exactly once per record
    # here instead of once per value-or-None expression per sample.
    fmt = record.FORMAT
    dp_arr = record.format("DP") if "DP" in fmt else None
    sq_arr = record.format("SQ") if "SQ" in fmt else None
    gt_ref_depths = record.gt_ref_depths
    gt_alt_depths = record.gt_alt_depths
    gt_types = record.gt_types
    gt_depths = record.gt_depths
    gt_alt_freqs = record.gt_alt_freqs
    gt_phases = record.gt_phases

    # Tumor FORMAT
    t_dp = dp_arr[tumor_index][0] if dp_arr is not None else 0
    t_ad_ref = gt_ref_depths[tumor_index] if gt_ref_depths[tumor_index] > 0 else None
    t_ad_alt = gt_alt_depths[tumor_index] if gt_alt_depths[tumor_index] > 0 else None
    t_calls = calls_without_phased(record, tumor_index)
    t_calls, t_zygosity = adjust_somatic_calls_and_zygosity(t_calls, gt_types[tumor_index], t_ad_alt)
    t_has_alt = 1 in t_calls
    t_ad_total = gt_depths[tumor_index] if gt_depths[tumor_index] > 0 else None
    t_ad_ratio = gt_alt_freqs[tumor_index] if gt_alt_freqs[tumor_index] > 0 else None
    t_af = t_ad_ratio
    t_phased = gt_phases[tumor_index]
    t_sq = sq_arr[tumor_index][0] if sq_arr is not None else None

    # Normal FORMAT — absent for a tumor-only analysis
    if normal_index is None:
        n_dp = n_ad_ref = n_ad_alt = n_calls = n_has_alt = None
        n_ad_total = n_ad_ratio = n_af = n_zygosity = n_phased = n_sq = None
    else:
        n_dp = dp_arr[normal_index][0] if dp_arr is not None else 0
        n_dp = n_dp if n_dp > 0 else None
        n_ad_ref = gt_ref_depths[normal_index] if gt_ref_depths[normal_index] > 0 else None
        n_ad_alt = gt_alt_depths[normal_index] if gt_alt_depths[normal_index] > 0 else None
        n_calls = calls_without_phased(record, normal_index)
        n_calls, n_zygosity = adjust_somatic_calls_and_zygosity(n_calls, gt_types[normal_index], n_ad_alt)
        n_has_alt = 1 in n_calls if n_calls is not None else None
        n_ad_total = gt_depths[normal_index] if gt_depths[normal_index] > 0 else None
        n_ad_ratio = gt_alt_freqs[normal_index] if gt_alt_freqs[normal_index] > 0 else None
        n_af = n_ad_ratio
        n_phased = gt_phases[normal_index]
        n_sq = sq_arr[normal_index][0] if sq_arr is not None else None

    occurrences[tumor_exp.seq_id] = {
        # common
        "part": common.part,
        "tenant_code": tumor_exp.tenant_code,
        "task_id": common.task_id,
        "locus": common.locus,
        "locus_hash": common.locus_hash,
        "chromosome": common.chromosome,
        "start": common.start,
        "end": common.end,
        "reference": common.reference,
        "alternate": common.alternate,
        # info
        "quality": info_quality,
        "filter": info_filter,
        "info_old_record": info_old_record,
        "info_baseq_rank_sum": baseq_ranksum,
        "info_excess_het": info_excess_het,
        "info_fs": info_fs,
        "info_ds": info_ds,
        "info_fraction_informative_reads": info_fraction_informative_reads,
        "info_inbreed_coeff": info_inbreed_coeff,
        "info_mleac": info_mleac,
        "info_mleaf": info_mleaf,
        "info_mq": info_mq,
        "info_mq0": info_mq0,
        "info_m_qrank_sum": info_mq_ranksum,
        "info_qd": info_qd,
        "info_r2_5p_bias": info_r2_5p_bias,
        "info_read_pos_rank_sum": info_read_pos_rank_su,
        "info_sor": info_sor,
        "info_vqslod": info_vqslod,
        "info_culprit": info_culprit,
        "info_dp": info_dp,
        "info_haplotype_score": info_haplotype_score,
        "info_hotspotallele": info_hotspotallele,
        "info_hotspot": info_hotspot,
        "info_cal": info_cal,
        "info_germq": info_germq,
        "info_tlod": info_tlod,
        "info_mapq": info_mapq,
        "info_aq": info_aq,
        # tumor FORMAT
        "tumor_seq_id": tumor_exp.seq_id,
        "tumor_calls": t_calls,
        "tumor_dp": t_dp if t_dp > 0 else None,
        "tumor_ad_ref": t_ad_ref,
        "tumor_ad_alt": t_ad_alt,
        "tumor_ad_total": t_ad_total,
        "tumor_ad_ratio": t_ad_ratio,
        "tumor_af": t_af,
        "tumor_zygosity": t_zygosity,
        "tumor_phased": t_phased,
        "tumor_has_alt": t_has_alt,
        "tumor_gt_status": None,
        "tumor_sq": t_sq,
        # normal FORMAT
        "normal_seq_id": normal_exp.seq_id if normal_exp is not None else None,
        "normal_calls": n_calls,
        "normal_dp": n_dp,
        "normal_ad_ref": n_ad_ref,
        "normal_ad_alt": n_ad_alt,
        "normal_ad_total": n_ad_total,
        "normal_ad_ratio": n_ad_ratio,
        "normal_af": n_af,
        "normal_zygosity": n_zygosity,
        "normal_phased": n_phased,
        "normal_has_alt": n_has_alt,
        "normal_gt_status": None,
        "normal_sq": n_sq,
    }

    return occurrences


def read_hotspot(info_fields) -> bool | None:
    """
    Resolve the hotspot indicator from whichever INFO key the caller emits.

    DRAGEN declares `hotspot` as a lowercase Flag, so cyvcf2 yields `True` when the site is a
    known somatic hotspot and nothing at all otherwise. GATK-era callers instead emit
    `HotspotAllele` as an allele index, where `1` designates the (single) alternate allele.

    Parameters:
        info_fields: The `record.INFO` mapping of the variant being processed.

    Returns:
        Optional[bool]: True/False when either key is present — `hotspot` wins over
        `HotspotAllele` — and None when neither is.
    """
    hotspot = info_fields.get("hotspot", None)
    if hotspot is not None:
        return bool(hotspot)

    hotspot_allele = info_fields.get("HotspotAllele", None)
    if hotspot_allele is not None:
        return hotspot_allele == 1

    return None


def adjust_somatic_calls_and_zygosity(calls: list[int], zygosity: int, ad_alt: int | None) -> tuple[list[int], str]:
    """
    Adjusts the somatic calls and zygosity based on alternate allele depth.

    Parameters:
        calls (list[int]): A list of genotype calls, where each call represents an allele (e.g., 0 for reference, 1 for alternate).
        zygosity (int): The zygosity type, represented as an integer (e.g., 0 for WT, 1 for HET, 3 for HOM, 2 for UNK).
        ad_alt (Optional[int]): The depth of reads supporting the alternate allele. Can be None if not available.

    Returns:
    Tuple[list[int], str]: A tuple containing:
        - The adjusted list of genotype calls.
        - The zygosity as a string (e.g., "WT", "HET", "HOM", "UNK", or "HEM").

    Behavior:

            For somatic variants, we determine presence based on allelic depth (AD)
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

    """  # noqa: E501
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
