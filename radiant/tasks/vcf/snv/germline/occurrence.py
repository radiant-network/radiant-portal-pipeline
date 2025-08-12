from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, StringType

from radiant.tasks.iceberg.utils import merge_schemas
from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.snv.germline.common import SCHEMA as COMMON_SCHEMA
from radiant.tasks.vcf.snv.germline.common import Common
from radiant.tasks.vcf.vcf_utils import ZYGOSITY, ZYGOSITY_HET, ZYGOSITY_HOM, ZYGOSITY_WT, calls_without_phased

SCHEMA = merge_schemas(
    COMMON_SCHEMA,
    Schema(
        NestedField(100, "part", IntegerType(), required=True),
        NestedField(101, "seq_id", IntegerType(), required=True),
        NestedField(102, "aliquot", StringType(), required=True),
        NestedField(104, "dp", IntegerType(), required=False),
        NestedField(105, "gq", IntegerType(), required=False),
        NestedField(106, "calls", ListType(202, IntegerType()), required=False),
        NestedField(107, "has_alt", BooleanType(), required=False),
        NestedField(108, "quality", FloatType(), required=False),
        NestedField(109, "filter", StringType(), required=False),
        NestedField(110, "info_old_record", StringType(), required=False),
        NestedField(111, "info_baseq_rank_sum", FloatType(), required=False),
        NestedField(112, "info_excess_het", FloatType(), required=False),
        NestedField(113, "info_fs", FloatType(), required=False),
        NestedField(114, "info_ds", BooleanType(), required=False),
        NestedField(115, "info_fraction_informative_reads", FloatType(), required=False),
        NestedField(116, "info_inbreed_coeff", FloatType(), required=False),
        NestedField(117, "info_mleac", IntegerType(), required=False),
        NestedField(118, "info_mleaf", FloatType(), required=False),
        NestedField(120, "info_mq", FloatType(), required=False),
        NestedField(121, "info_m_qrank_sum", FloatType(), required=False),
        NestedField(122, "info_qd", FloatType(), required=False),
        NestedField(123, "info_r2_5p_bias", FloatType(), required=False),
        NestedField(124, "info_read_pos_rank_sum", FloatType(), required=False),
        NestedField(125, "info_sor", FloatType(), required=False),
        NestedField(126, "info_vqslod", FloatType(), required=False),
        NestedField(127, "info_culprit", StringType(), required=False),
        NestedField(128, "info_dp", IntegerType(), required=False),
        NestedField(129, "info_haplotype_score", FloatType(), required=False),
        NestedField(133, "zygosity", StringType(), required=False),
        NestedField(134, "ad_ref", IntegerType(), required=False),
        NestedField(135, "ad_alt", IntegerType(), required=False),
        NestedField(136, "ad_total", IntegerType(), required=False),
        NestedField(137, "ad_ratio", FloatType(), required=False),
        NestedField(138, "phased", BooleanType(), required=True),
        NestedField(139, "parental_origin", StringType(), required=False),
        NestedField(140, "father_dp", IntegerType(), required=False),
        NestedField(141, "father_gq", IntegerType(), required=False),
        NestedField(143, "father_ad_ref", IntegerType(), required=False),
        NestedField(144, "father_ad_alt", IntegerType(), required=False),
        NestedField(145, "father_ad_total", IntegerType(), required=False),
        NestedField(146, "father_ad_ratio", FloatType(), required=False),
        NestedField(147, "father_calls", ListType(247, IntegerType()), required=False),
        NestedField(148, "father_zygosity", StringType(), required=False),
        NestedField(149, "mother_dp", IntegerType(), required=False),
        NestedField(150, "mother_gq", IntegerType(), required=False),
        NestedField(152, "mother_ad_ref", IntegerType(), required=False),
        NestedField(153, "mother_ad_alt", IntegerType(), required=False),
        NestedField(154, "mother_ad_total", IntegerType(), required=False),
        NestedField(155, "mother_ad_ratio", FloatType(), required=False),
        NestedField(156, "mother_calls", ListType(256, IntegerType()), required=False),
        NestedField(157, "mother_zygosity", StringType(), required=False),
        NestedField(158, "transmission_mode", StringType(), required=False),
        NestedField(159, "task_id", IntegerType(), required=True),
    ),
)


def process_occurrence(record: Variant, ped: Pedigree, common: Common) -> dict:
    """
    Processes a genetic variant occurrence and extracts relevant information for each sample in the pedigree.

    Parameters:
        record (Variant): A `cyvcf2.Variant` object representing the genetic variant to process.
        ped (Pedigree): A `Pedigree` object containing information about the case and its associated samples.
        common (Common): A `Common` object containing shared attributes for the variant, such as locus and chromosome.

    Returns:
        dict: A dictionary where each key is a sample's `seq_id` and the value is a dictionary of extracted attributes
              for that sample. The attributes include genotype calls, zygosity, depth of coverage, parental origin,
              and transmission mode, among others. Example :
              ```
                  {
                        1001: {
                            "case_id": "case_001",
                            "seq_id": 1001,
                            "calls": [0, 1],
                            "locus": "chr1:1000-2000",
                            "locus_hash": "abc123",
                            "chromosome": "1",
                            "start": 1000,
                            "end": 2000,
                            "reference": "A",
                            "alternate": "T",
                            "aliquot": "SA0001",
                            "dp": 30,
                            "gq": 99,
                            ...
                        },
                        1002: {
                            "case_id": "case_001",
                            "seq_id": 1002,
                            "calls": [0, 1],
                            ...
                        },
                        1003: {
                            "case_id": "case_001",
                            "seq_id": 1003,
                            "calls": [0, 0],
                            ...
                        },
                }
            ```

    Behavior:
        - Extracts sample-specific attributes such as `dp`, `gq`, `calls`, `zygosity`, and allele depths.
        - Computes parental origin and transmission mode for family-based pedigrees.
        - Handles missing or invalid data gracefully by setting appropriate default values.
    """
    occurrences = {}

    info_fields = record.INFO
    quality = int(record.QUAL) if record.QUAL is not None else None
    filter = record.FILTER or "PASS"
    old_record = info_fields.get("OLD_RECORD", None)
    baseq_ranksum = info_fields.get("BaseQRankSum", None)
    fs = info_fields.get("FS", None)
    ds = info_fields.get("DS", None)
    fraction_informative_reads = info_fields.get("FractionInformativeReads", None)
    inbreed_coeff = info_fields.get("InbreedCoeff", None)
    mleac = info_fields.get("MLEAC", None)
    mleaf = info_fields.get("MLEAF", None)
    mq = info_fields.get("MQ", None)
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

    for idx, exp in enumerate(ped.experiments):
        dp = record.format("DP")[idx][0] if "DP" in record.FORMAT else 0
        gq = record.format("GQ")[idx][0] if "GQ" in record.FORMAT else 0
        ad_ref = record.gt_ref_depths[idx] if record.gt_ref_depths[idx] > 0 else None
        ad_alt = record.gt_alt_depths[idx] if record.gt_alt_depths[idx] > 0 else None
        calls = calls_without_phased(record, idx)
        calls, zygosity = adjust_calls_and_zygosity(calls, record.gt_types[idx], ad_ref, ad_alt)

        has_alt = 1 in calls
        occurrences[exp.seq_id] = {
            "case_id": common.case_id,
            "part": common.part,
            "seq_id": exp.seq_id,
            "task_id": exp.task_id,
            "locus": common.locus,
            "locus_hash": common.locus_hash,
            "chromosome": common.chromosome,
            "start": common.start,
            "end": common.end,
            "reference": common.reference,
            "alternate": common.alternate,
            "aliquot": exp.aliquot,
            "dp": dp if dp > 0 else None,
            "gq": gq if gq > 0 else None,
            "calls": calls,
            "has_alt": has_alt,
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
            "info_m_qrank_sum": mq_ranksum,
            "info_qd": qd,
            "info_r2_5p_bias": r2_5p_bias,
            "info_read_pos_rank_sum": read_pos_rank_su,
            "info_sor": sor,
            "info_vqslod": vqslod,
            "info_culprit": culprit,
            "info_dp": info_dp,
            "info_haplotype_score": haplotype_score,
            "zygosity": zygosity,
            "ad_ref": ad_ref,
            "ad_alt": ad_alt,
            "ad_total": record.gt_depths[idx] if record.gt_depths[idx] > 0 else None,
            "ad_ratio": (record.gt_alt_freqs[idx] if record.gt_alt_freqs[idx] > 0 else None),
            "phased": record.gt_phases[idx],
        }

    if ped.is_family:
        father_occurrence = occurrences.get(ped.father_seq_id, {})
        mother_occurrence = occurrences.get(ped.mother_seq_id, {})
        normalized_father_calls = normalize_calls(father_occurrence.get("calls"))
        normalized_mother_calls = normalize_calls(mother_occurrence.get("calls"))
        for progeny in ped.progenies:
            progeny_occurrence = occurrences[progeny.seq_id]
            normalized_progeny_calls = normalize_calls(progeny_occurrence["calls"])
            po = parental_origin(
                common.chromosome,
                normalized_progeny_calls,
                normalized_father_calls,
                normalized_mother_calls,
            )
            transmission_mode = compute_transmission_mode(
                common.chromosome,
                progeny.sex,
                normalized_progeny_calls,
                normalized_father_calls,
                normalized_mother_calls,
                ped.is_father_affected,
                ped.is_mother_affected,
            )
            progeny_occurrence["parental_origin"] = po
            progeny_occurrence["transmission_mode"] = transmission_mode
            progeny_occurrence["father_dp"] = father_occurrence.get("dp")
            progeny_occurrence["father_gq"] = father_occurrence.get("gq")
            progeny_occurrence["father_ad_ref"] = father_occurrence.get("ad_ref")
            progeny_occurrence["father_ad_alt"] = father_occurrence.get("ad_alt")
            progeny_occurrence["father_ad_total"] = father_occurrence.get("ad_total")
            progeny_occurrence["father_ad_ratio"] = father_occurrence.get("ad_ratio")
            progeny_occurrence["father_calls"] = father_occurrence.get("calls")
            progeny_occurrence["father_zygosity"] = father_occurrence.get("zygosity")
            progeny_occurrence["mother_dp"] = mother_occurrence.get("dp")
            progeny_occurrence["mother_gq"] = mother_occurrence.get("gq")
            progeny_occurrence["mother_ad_ref"] = mother_occurrence.get("ad_ref")
            progeny_occurrence["mother_ad_alt"] = mother_occurrence.get("ad_alt")
            progeny_occurrence["mother_ad_total"] = mother_occurrence.get("ad_total")
            progeny_occurrence["mother_ad_ratio"] = mother_occurrence.get("ad_ratio")
            progeny_occurrence["mother_calls"] = mother_occurrence.get("calls")
            progeny_occurrence["mother_zygosity"] = mother_occurrence.get("zygosity")

    return occurrences


def adjust_calls_and_zygosity(
    calls: list[int], zygosity: int, ad_ref: int | None, ad_alt: int | None
) -> tuple[list[int], str]:
    """

    Adjusts the calls and zygosity based on the reference and alternate allele depths.

    Parameters:
        calls (list[int]): A list of genotype calls, where each call represents an allele
        (e.g., 0 for reference, 1 for alternate).
        zygosity (int): The zygosity type, represented as an integer (e.g., 0 for WT, 1 for HET, 3 for HOM, 2 for UNK).
        ad_ref (Optional[int]): The depth of reads supporting the reference allele. Can be None if not available.
        ad_alt (Optional[int]): The depth of reads supporting the alternate allele. Can be None if not available.

    Returns:
        Tuple[list[int], str]: A tuple containing:
            - The adjusted list of genotype calls.
            - The zygosity as a string (e.g., "WT", "HET", "HOM", "UNK", or "HEM").

    Behavior:
        - If the alternate allele depth (`ad_alt`) is less than 3 and the zygosity is HET or HOM, the calls
        are adjusted to [-1, -1] and the zygosity is set to "UNK".
        - If the reference allele depth (`ad_ref`) is less than 3 and the zygosity is WT, the calls
        are adjusted to [-1, -1] and the zygosity is set to "UNK".
        - If the zygosity is HOM and there is only one call, the zygosity is adjusted to "HEM" (hemizygous).
        - Otherwise, the calls remain unchanged, and the zygosity is returned as its string representation.
    """
    if (
        ad_alt
        and (zygosity in (ZYGOSITY_HET, ZYGOSITY_HOM) and ad_alt < 3)
        or ad_ref
        and zygosity == ZYGOSITY_WT
        and ad_ref < 3
    ):
        return [-1 for _ in range(len(calls))], "UNK"
    elif zygosity == 3 and len(calls) == 1:
        return calls, "HEM"
    else:
        return calls, ZYGOSITY[zygosity]


def normalize_calls(calls):
    """
    Normalizes a list of genotype calls by sorting them in ascending order.

    Parameters:
        calls (list[int] or None): A list of genotype calls, where each call represents an allele
                                   (e.g., 0 for reference, 1 for alternate). Can be None.

    Returns:
        tuple[int] or None: A tuple of sorted genotype calls if `calls` is not None; otherwise, None.

    Behavior:
        - If `calls` is None, the function returns None.
        - If `calls` is a list, it is sorted in ascending order and returned as a tuple.
    """
    if calls is None:
        return None
    else:
        return tuple(sorted(calls))


def normalize_monosomy(calls: tuple):
    """
    Normalizes monosomy genotype calls by duplicating the single allele call.

    Parameters:
        calls (tuple): A tuple representing genotype calls. It can contain one or two elements:
                       - A single element (e.g., (1,)) for monosomy.
                       - Two elements (e.g., (0, 1)) for diploidy.

    Returns:
        tuple: A tuple with normalized genotype calls:
               - If the input contains one element, it is duplicated (e.g., (1,) -> (1, 1)).
               - If the input contains two elements, it is returned unchanged.

    Behavior:
        - Monosomy calls are duplicated to ensure compatibility with diploid-based processing.
        - Diploid calls remain unchanged.
    """
    if len(calls) == 1:
        return calls[0], calls[0]
    else:
        return calls


def parental_origin(
    chromosome,
    normalized_progeny_calls: tuple,
    normalized_father_calls: tuple,
    normalized_mother_calls: tuple,
):
    """
    Determines the parental origin of a genetic variant based on the genotype calls of the progeny, father, and mother.

    Parameters:
        chromosome (str): The chromosome where the variant is located. Can be "X", "Y", or autosomal (e.g., "1", "2").
        normalized_progeny_calls (tuple): The normalized genotype calls of the progeny.
        normalized_father_calls (tuple): The normalized genotype calls of the father.
        normalized_mother_calls (tuple): The normalized genotype calls of the mother.

    Returns:
        str: A string representing the parental origin of the variant. Possible values include:
            - "DENOVO": The variant is de novo (not inherited from either parent).
            - "FATHER": The variant is inherited from the father.
            - "MOTHER": The variant is inherited from the mother.
            - "BOTH": The variant is inherited from both parents.
            - "UNKNOWN": The parental origin could not be determined.
            - Other specific values based on lookup tables.

    Behavior:
        - If both parental genotypes are unknown, returns "UNKNOWN".
        - Uses lookup tables to determine the parental origin based on the chromosome type (autosomal, X, or Y).
    """
    if normalized_father_calls is None and normalized_mother_calls is None:
        return UNKNOWN
    key = (
        normalized_progeny_calls,
        normalized_father_calls if normalized_father_calls else (-1, -1),
        normalized_mother_calls if normalized_mother_calls else (-1, -1),
    )

    if chromosome == "Y":
        return Y_ORIGINS_LOOKUP.get(key, UNKNOWN)
    elif chromosome == "X":
        return X_ORIGINS_LOOKUP.get(key, UNKNOWN)
    else:
        return AUTOSOMAL_ORIGINS_LOOKUP.get(key, UNKNOWN)


def compute_transmission_mode(
    chromosome: str,
    gender: str,
    normalized_progeny_calls: tuple[int, int] | None,
    normalized_father_calls: tuple[int, int] | None,
    normalized_mother_calls: tuple[int, int] | None,
    father_affected: bool,
    mother_affected: bool,
) -> str | None:
    """
    Computes the transmission mode of a genetic variant based on the genotype calls of the progeny, father, and mother.

    Parameters:
        chromosome (str): The chromosome where the variant is located. Can be "X", "Y", or autosomal (e.g., "1", "2").
        gender (str): The gender of the progeny. Expected values are "Male" or "Female".
        normalized_progeny_calls (Optional[Tuple[int, int]]): The normalized genotype calls of the progeny.
        normalized_father_calls (Optional[Tuple[int, int]]): The normalized genotype calls of the father.
        normalized_mother_calls (Optional[Tuple[int, int]]): The normalized genotype calls of the mother.
        father_affected (bool): Indicates whether the father is affected by the condition.
        mother_affected (bool): Indicates whether the mother is affected by the condition.

    Returns:
        Optional[str]: A string representing the transmission mode. Possible values include:
            - "autosomal_dominant"
            - "autosomal_recessive"
            - "x_linked_dominant"
            - "x_linked_recessive"
            - "unknown_parents_genotype"
            - "unknown_father_genotype"
            - "unknown_mother_genotype"
            - "unknown_proband_genotype"
            - "non_carrier_proband"
            - Other specific modes based on lookup tables.

    Behavior:
        - If both parental genotypes are unknown, returns "unknown_parents_genotype".
        - If only the father's genotype is unknown, returns "unknown_father_genotype".
        - If only the mother's genotype is unknown, returns "unknown_mother_genotype".
        - If the progeny's genotype is unknown or invalid, returns "unknown_proband_genotype".
        - If the progeny is a non-carrier (homozygous reference), returns "non_carrier_proband".
        - For valid genotypes, determines the transmission mode using lookup tables based on the chromosome type.

    Notes:
        - Monosomy calls are normalized to ensure compatibility with diploid-based processing.
        - The function uses separate lookup tables for autosomal and sex chromosomes (X and Y).
    """

    # Step 1: static checks
    if normalized_father_calls is None and normalized_mother_calls is None:
        return "unknown_parents_genotype"
    if normalized_father_calls is None:
        return "unknown_father_genotype"
    if normalized_mother_calls is None:
        return "unknown_mother_genotype"

    # Normalize monosomy
    norm_calls = normalize_monosomy(normalized_progeny_calls)
    norm_fth = normalize_monosomy(normalized_father_calls)
    norm_mth = normalize_monosomy(normalized_mother_calls)

    if norm_calls == (0, 0):
        return "non_carrier_proband"
    if norm_calls is None or norm_calls == (-1, -1):
        return "unknown_proband_genotype"

    # Step 2: use lookup
    if chromosome in ("X", "Y"):
        result = SEXUAL_TRANSMISSION_LOOKUP.get(
            (gender, norm_calls, norm_fth, norm_mth, father_affected, mother_affected)
        )
    else:
        result = AUTOSOMAL_TRANSMISSION_LOOKUP.get((norm_calls, norm_fth, norm_mth, father_affected, mother_affected))

    return result


DENOVO = "DENOVO"
MTH = "MOTHER"
FTH = "FATHER"
BOTH = "BOTH"
AMBIGUOUS = "AMBIGUOUS"
POSSIBLE_DENOVO = "POSSIBLE_DENOVO"
POSSIBLE_MOTHER = "POSSIBLE_MOTHER"
POSSIBLE_FATHER = "POSSIBLE_FATHER"
UNKNOWN = "UNKNOWN"

AUTOSOMAL_ORIGINS_LOOKUP = {
    (tuple([0, 1]), tuple([0, 0]), tuple([0, 0])): DENOVO,
    (tuple([0, 1]), tuple([0, 0]), tuple([0, 1])): MTH,
    (tuple([0, 1]), tuple([0, 0]), tuple([1, 1])): MTH,
    (tuple([0, 1]), tuple([0, 0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([0, 1]), tuple([0, 1]), tuple([0, 0])): FTH,
    (tuple([0, 1]), tuple([0, 1]), tuple([0, 1])): AMBIGUOUS,
    (tuple([0, 1]), tuple([0, 1]), tuple([1, 1])): MTH,
    (tuple([0, 1]), tuple([0, 1]), tuple([-1, -1])): POSSIBLE_FATHER,
    (tuple([0, 1]), tuple([1, 1]), tuple([0, 0])): FTH,
    (tuple([0, 1]), tuple([1, 1]), tuple([0, 1])): FTH,
    (tuple([0, 1]), tuple([1, 1]), tuple([1, 1])): AMBIGUOUS,
    (tuple([0, 1]), tuple([1, 1]), tuple([-1, -1])): FTH,
    (tuple([0, 1]), tuple([-1, -1]), tuple([0, 0])): POSSIBLE_DENOVO,
    (tuple([0, 1]), tuple([-1, -1]), tuple([0, 1])): POSSIBLE_MOTHER,
    (tuple([0, 1]), tuple([-1, -1]), tuple([1, 1])): MTH,
    (tuple([0, 1]), tuple([-1, -1]), tuple([-1, -1])): UNKNOWN,
    (tuple([1, 1]), tuple([0, 0]), tuple([0, 0])): DENOVO,
    (tuple([1, 1]), tuple([0, 0]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([0, 0]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([0, 0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([1, 1]), tuple([0, 1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([0, 1])): BOTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([1, 1])): BOTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([-1, -1])): FTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([0, 1])): BOTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([1, 1])): BOTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([-1, -1])): FTH,
    (tuple([1, 1]), tuple([-1, -1]), tuple([0, 0])): POSSIBLE_DENOVO,
    (tuple([1, 1]), tuple([-1, -1]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([-1, -1]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([-1, -1]), tuple([-1, -1])): UNKNOWN,
}

X_ORIGINS_LOOKUP = {
    (tuple([0, 1]), tuple([0]), tuple([0, 0])): DENOVO,
    (tuple([1]), tuple([0]), tuple([0, 0])): DENOVO,
    (tuple([1, 1]), tuple([0]), tuple([0, 0])): DENOVO,
    (tuple([0, 1]), tuple([0]), tuple([0, 1])): MTH,
    (tuple([1]), tuple([0]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([0]), tuple([0, 1])): MTH,
    (tuple([0, 1]), tuple([0]), tuple([1, 1])): MTH,
    (tuple([1]), tuple([0]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([0]), tuple([1, 1])): MTH,
    (tuple([0, 1]), tuple([1]), tuple([0, 0])): FTH,
    (tuple([1]), tuple([1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([1]), tuple([0, 0])): FTH,
    (tuple([0, 1]), tuple([1]), tuple([0, 1])): FTH,
    (tuple([1]), tuple([1]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([1]), tuple([0, 1])): BOTH,
    (tuple([0, 1]), tuple([1]), tuple([1, 1])): AMBIGUOUS,
    (tuple([1]), tuple([1]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([1]), tuple([1, 1])): BOTH,
    (tuple([0, 1]), tuple([0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([0, 1]), tuple([1]), tuple([-1, -1])): FTH,
    (tuple([0, 1]), tuple([-1]), tuple([0, 0])): POSSIBLE_DENOVO,
    (tuple([0, 1]), tuple([-1]), tuple([0, 1])): POSSIBLE_MOTHER,
    (tuple([0, 1]), tuple([-1]), tuple([1, 1])): MTH,
    (tuple([0, 1]), tuple([-1]), tuple([-1, -1])): UNKNOWN,
    (tuple([0, 1]), tuple([-1, -1]), tuple([-1, -1])): UNKNOWN,
    (tuple([1, 1]), tuple([0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([1, 1]), tuple([1]), tuple([-1, -1])): FTH,
    (tuple([1, 1]), tuple([-1]), tuple([0, 0])): POSSIBLE_DENOVO,
    (tuple([1, 1]), tuple([-1]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([-1]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([-1]), tuple([-1, -1])): UNKNOWN,
    (tuple([1, 1]), tuple([-1, -1]), tuple([-1, -1])): UNKNOWN,
    (tuple([1]), tuple([0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([1]), tuple([1]), tuple([-1, -1])): POSSIBLE_FATHER,
    (tuple([1]), tuple([-1]), tuple([0, 0])): POSSIBLE_DENOVO,
    (tuple([1]), tuple([-1]), tuple([0, 1])): MTH,
    (tuple([1]), tuple([-1]), tuple([1, 1])): MTH,
    (tuple([1]), tuple([-1]), tuple([-1, -1])): UNKNOWN,
    (tuple([1]), tuple([-1, -1]), tuple([-1, -1])): UNKNOWN,
    (tuple([0, 1]), tuple([0, 0]), tuple([0, 0])): DENOVO,
    (tuple([1]), tuple([0, 0]), tuple([0, 0])): DENOVO,
    (tuple([1, 1]), tuple([0, 0]), tuple([0, 0])): DENOVO,
    (tuple([0, 1]), tuple([0, 0]), tuple([0, 1])): MTH,
    (tuple([1]), tuple([0, 0]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([0, 0]), tuple([0, 1])): MTH,
    (tuple([0, 1]), tuple([0, 0]), tuple([1, 1])): MTH,
    (tuple([1]), tuple([0, 0]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([0, 0]), tuple([1, 1])): MTH,
    (tuple([0, 1]), tuple([0, 1]), tuple([0, 0])): FTH,
    (tuple([0, 1]), tuple([1, 1]), tuple([0, 0])): FTH,
    (tuple([1]), tuple([0, 1]), tuple([0, 0])): FTH,
    (tuple([1]), tuple([1, 1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([0, 0])): FTH,
    (tuple([0, 1]), tuple([0, 1]), tuple([0, 1])): AMBIGUOUS,
    (tuple([0, 1]), tuple([1, 1]), tuple([0, 1])): FTH,
    (tuple([1]), tuple([0, 1]), tuple([0, 1])): AMBIGUOUS,
    (tuple([1]), tuple([1, 1]), tuple([0, 1])): AMBIGUOUS,
    (tuple([1, 1]), tuple([0, 1]), tuple([0, 1])): BOTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([0, 1])): BOTH,
    (tuple([0, 1]), tuple([0, 1]), tuple([1, 1])): AMBIGUOUS,
    (tuple([0, 1]), tuple([1, 1]), tuple([1, 1])): AMBIGUOUS,
    (tuple([1]), tuple([0, 1]), tuple([1, 1])): MTH,
    (tuple([1]), tuple([1, 1]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([1, 1])): BOTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([1, 1])): BOTH,
    (tuple([0, 1]), tuple([0, 0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([0, 1]), tuple([0, 1]), tuple([-1, -1])): FTH,
    (tuple([0, 1]), tuple([1, 1]), tuple([-1, -1])): FTH,
    (tuple([1, 1]), tuple([0, 0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([1, 1]), tuple([0, 1]), tuple([-1, -1])): FTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([-1, -1])): FTH,
    (tuple([1]), tuple([0, 0]), tuple([-1, -1])): POSSIBLE_DENOVO,
    (tuple([1]), tuple([0, 1]), tuple([-1, -1])): POSSIBLE_FATHER,
    (tuple([1]), tuple([1, 1]), tuple([-1, -1])): POSSIBLE_FATHER,
}

Y_ORIGINS_LOOKUP = {
    (tuple([1]), tuple([1]), tuple([-1, -1])): FTH,
    (tuple([1]), tuple([0]), tuple([-1, -1])): DENOVO,
    (tuple([1, 1]), tuple([1]), tuple([-1, -1])): FTH,
    (tuple([1]), tuple([1, 1]), tuple([-1, -1])): FTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([-1, -1])): FTH,
    (tuple([1, 1]), tuple([0]), tuple([-1, -1])): DENOVO,
    (tuple([1]), tuple([0, 0]), tuple([-1, -1])): DENOVO,
    (tuple([1, 1]), tuple([0, 0]), tuple([-1, -1])): DENOVO,
    (tuple([1]), tuple([-1, -1]), tuple([-1, -1])): UNKNOWN,
    (tuple([1, 1]), tuple([-1, -1]), tuple([-1, -1])): UNKNOWN,
    (tuple([1]), tuple([1]), tuple([0, 0])): FTH,
    (tuple([1]), tuple([1]), tuple([0, 1])): AMBIGUOUS,
    (tuple([1]), tuple([1, 1]), tuple([1, 1])): AMBIGUOUS,
    (tuple([1]), tuple([1, 1]), tuple([0, 0])): FTH,
    (tuple([1]), tuple([1, 1]), tuple([0, 1])): AMBIGUOUS,
    (tuple([1]), tuple([0, 1]), tuple([0, 0])): FTH,
    (tuple([1]), tuple([0, 1]), tuple([0, 1])): AMBIGUOUS,
    (tuple([1]), tuple([0, 1]), tuple([1, 1])): AMBIGUOUS,
    (tuple([1]), tuple([0]), tuple([0, 0])): DENOVO,
    (tuple([1]), tuple([0]), tuple([0, 1])): MTH,
    (tuple([1]), tuple([0, 0]), tuple([0, 0])): DENOVO,
    (tuple([1]), tuple([0, 0]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([1]), tuple([0, 1])): BOTH,
    (tuple([1, 1]), tuple([1]), tuple([1, 1])): BOTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([0, 1])): BOTH,
    (tuple([1, 1]), tuple([1, 1]), tuple([1, 1])): BOTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([0, 0])): FTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([0, 1])): BOTH,
    (tuple([1, 1]), tuple([0, 1]), tuple([1, 1])): BOTH,
    (tuple([1, 1]), tuple([0]), tuple([0, 0])): DENOVO,
    (tuple([1, 1]), tuple([0]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([0]), tuple([1, 1])): MTH,
    (tuple([1, 1]), tuple([0, 0]), tuple([0, 0])): DENOVO,
    (tuple([1, 1]), tuple([0, 0]), tuple([0, 1])): MTH,
    (tuple([1, 1]), tuple([0, 0]), tuple([1, 1])): MTH,
    (tuple([1]), tuple([0, 0]), tuple([1, 1])): MTH,
}

AUTOSOMAL_TRANSMISSION_LOOKUP = {
    ((0, 1), (0, 0), (0, 0), False, False): "autosomal_dominant_de_novo",
    ((0, 1), (0, 0), (0, 1), False, True): "autosomal_dominant",
    ((0, 1), (0, 0), (1, 1), False, True): "autosomal_dominant",
    ((0, 1), (0, 1), (0, 0), True, False): "autosomal_dominant",
    ((0, 1), (0, 1), (1, 1), True, True): "autosomal_dominant",
    ((0, 1), (0, 1), (0, 1), True, True): "autosomal_dominant",
    ((0, 1), (0, 1), (-1, -1), True, True): "autosomal_dominant",
    ((0, 1), (0, 1), (-1, -1), True, False): "autosomal_dominant",
    ((0, 1), (1, 1), (0, 0), True, True): "autosomal_dominant",
    ((0, 1), (1, 1), (0, 0), True, False): "autosomal_dominant",
    ((0, 1), (1, 1), (0, 1), True, True): "autosomal_dominant",
    ((0, 1), (1, 1), (0, 1), True, False): "autosomal_dominant",
    ((0, 1), (1, 1), (1, 1), True, True): "autosomal_dominant",
    ((0, 1), (1, 1), (1, 1), True, False): "autosomal_dominant",
    ((0, 1), (1, 1), (-1, -1), True, True): "autosomal_dominant",
    ((0, 1), (1, 1), (-1, -1), True, False): "autosomal_dominant",
    ((0, 1), (-1, -1), (0, 1), True, True): "autosomal_dominant",
    ((0, 1), (-1, -1), (0, 1), False, True): "autosomal_dominant",
    ((0, 1), (-1, -1), (1, 1), True, True): "autosomal_dominant",
    ((0, 1), (-1, -1), (1, 1), False, True): "autosomal_dominant",
    ((1, 1), (0, 1), (0, 1), False, False): "autosomal_recessive",
    ((1, 1), (0, 1), (1, 1), False, True): "autosomal_recessive",
    ((1, 1), (1, 1), (0, 1), True, False): "autosomal_recessive",
    ((1, 1), (1, 1), (1, 1), True, True): "autosomal_recessive",
}

SEXUAL_TRANSMISSION_LOOKUP = {
    ("Female", (0, 1), (0, 0), (0, 0), False, False): "x_linked_dominant_de_novo",
    ("Male", (1, 1), (0, 0), (0, 0), False, False): "x_linked_recessive_de_novo",
    ("Female", (0, 1), (0, 0), (0, 1), False, True): "x_linked_dominant",
    ("Male", (1, 1), (0, 0), (0, 1), False, False): "x_linked_recessive",
    ("Male", (1, 1), (0, 0), (1, 1), False, True): "x_linked_recessive",
    ("Female", (0, 1), (1, 1), (0, 0), True, False): "x_linked_dominant",
    ("Male", (1, 1), (1, 1), (0, 0), True, False): "x_linked_recessive",
    ("Female", (0, 1), (1, 1), (0, 1), True, True): "x_linked_dominant",
    ("Male", (1, 1), (1, 1), (0, 1), True, False): "x_linked_recessive",
    ("Male", (0, 1), (1, 1), (0, 1), True, True): "x_linked_recessive",
    ("Male", (1, 1), (1, 1), (1, 1), True, True): "x_linked_recessive",
    ("Female", (0, 1), (-1, -1), (0, 1), False, True): "x_linked_dominant",
    ("Female", (0, 1), (-1, -1), (0, 1), True, True): "x_linked_dominant",
    ("Male", (1, 1), (-1, -1), (0, 1), True, False): "x_linked_recessive",
    ("Male", (1, 1), (-1, -1), (0, 1), False, False): "x_linked_recessive",
    ("Male", (1, 1), (-1, -1), (1, 1), False, True): "x_linked_recessive",
    ("Male", (1, 1), (-1, -1), (1, 1), True, True): "x_linked_recessive",
    ("Female", (1, 1), (1, 1), (0, 0), True, False): "x_linked_recessive",
    ("Female", (1, 1), (1, 1), (0, 1), True, False): "x_linked_recessive",
    ("Female", (1, 1), (1, 1), (1, 1), True, True): "x_linked_recessive",
    ("Female", (1, 1), (1, 1), (-1, -1), True, True): "x_linked_dominant",
    ("Female", (1, 1), (1, 1), (-1, -1), True, False): "x_linked_dominant",
}
