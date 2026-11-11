import logging

from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, StringType

from radiant.tasks.vcf.vcf_utils import calls_without_phased, format_value, format_values

logger = logging.getLogger("airflow.task")

SCHEMA: Schema = Schema(
    NestedField(600, "part", IntegerType(), required=True),
    NestedField(601, "seq_id", IntegerType(), required=True),
    NestedField(602, "tenant_code", StringType(), required=True),
    NestedField(603, "task_id", IntegerType(), required=True),
    NestedField(604, "aliquot", StringType(), required=True),
    NestedField(605, "chromosome", StringType(), required=True),
    NestedField(606, "alternate", StringType(), required=True),
    NestedField(607, "start", IntegerType(), required=True),
    NestedField(608, "end", IntegerType(), required=True),
    NestedField(609, "type", StringType(), required=True),
    NestedField(610, "length", IntegerType(), required=True),
    NestedField(611, "name", StringType(), required=True),
    NestedField(612, "quality", FloatType(), required=False),
    NestedField(613, "calls", ListType(700, IntegerType()), required=False),
    NestedField(614, "filter", StringType(), required=False),
    NestedField(615, "bc", IntegerType(), required=False),
    # NULL, or every value present: `format_values` rejects a partly missing vector.
    NestedField(616, "pe", ListType(701, IntegerType()), required=False),
    NestedField(617, "sm", FloatType(), required=False),
    NestedField(618, "svtype", StringType(), required=False),
    NestedField(619, "svlen", IntegerType(), required=False),
    NestedField(620, "reflen", IntegerType(), required=False),
    NestedField(621, "ciend", ListType(702, IntegerType()), required=False),
    NestedField(622, "cipos", ListType(703, IntegerType()), required=False),
    NestedField(623, "phased", BooleanType(), required=False),
    # DRAGEN 4.2.4 allele-specific copy number (ASCN) fields. Absent altogether on 3.10.8 and
    # declared-but-omitted per record on 4.2.4, hence all nullable and read defensively below.
    NestedField(624, "cn", IntegerType(), required=False),
    NestedField(625, "cnf", FloatType(), required=False),
    NestedField(626, "cnq", FloatType(), required=False),
    NestedField(627, "mcn", IntegerType(), required=False),
    NestedField(628, "mcnf", FloatType(), required=False),
    NestedField(629, "mcnq", FloatType(), required=False),
    NestedField(630, "maf", FloatType(), required=False),
    NestedField(631, "sd", FloatType(), required=False),
    # DRAGEN spells this FORMAT field `AS`, which is a reserved word in both Python and
    # StarRocks/MySQL; storing it under a prefixed name keeps every downstream call site unquoted.
    NestedField(632, "ascn_as", IntegerType(), required=False),
)

GAIN = "GAIN"
LOSS = "LOSS"
CNLOH = "CNLOH"
GAINLOH = "GAINLOH"

CNV_TYPES = (GAIN, LOSS, CNLOH, GAINLOH)
LOH_TYPES = (CNLOH, GAINLOH)

ALT_DUP = "<DUP>"
ALT_DEL = "<DEL>"
ALT_LOH = "<LOH>"

# What the stored `alternate` is, per resolved type. Deriving it from `type` rather than from the raw
# ALT is what makes the two DRAGEN spellings of an LOH event produce the same row: VCF 4.2 writes
# `<DEL>,<DUP>` and VCF 4.4 writes `<LOH>` for the same segment.
ALTERNATE_BY_TYPE = {
    GAIN: ALT_DUP,
    LOSS: ALT_DEL,
    CNLOH: ALT_LOH,
    GAINLOH: ALT_LOH,
}

# `type` implied by the ALT alone, used only when the DRAGEN ID is unparseable. LOH is deliberately
# absent: two ALTs (4.2) or `<LOH>` (4.4) say the event *is* LOH but not which kind.
_TYPE_BY_ALT = {
    (ALT_DUP,): GAIN,
    (ALT_DEL,): LOSS,
}
_LOH_ALTS = ((ALT_LOH,), (ALT_DEL, ALT_DUP), (ALT_DUP, ALT_DEL))


def resolve_cnv_type(record: Variant) -> str | None:
    """Resolves the CNV type of a record, or `None` when the record must be skipped.

    The DRAGEN ID (`DRAGEN:<TYPE>:<chr>:<start>-<end>`) is the primary and, in practice, the only
    source: `CN`/`MCN` are either not emitted at all (DRAGEN 3.10.8) or omitted from the LOH record's
    own FORMAT (4.2.4), and ALT cannot express `CNLOH` versus `GAINLOH`. This is a knowing coupling to
    a vendor convention -- the VCF spec treats ID as optional -- accepted because nothing else in the
    file can classify LOH.

    An unrecognised ALT is skipped before the ID is even read: a type is only meaningful for an ALT we
    understand, and `cnv_id` is a `NOT NULL` key column downstream, so a record we cannot classify must
    never be persisted rather than degrade to an `UNKNOWN` row that fails the whole StarRocks load.

    Args:
        record (Variant): A `cyvcf2.Variant` object representing the CNV segment.

    Returns:
        str | None: One of `CNV_TYPES`, or `None` when the record should be skipped.

    Raises:
        ValueError: The ALT says the segment is LOH but the ID does not say which kind.
    """
    alts = tuple(record.ALT)
    is_loh_alt = alts in _LOH_ALTS
    if not is_loh_alt and alts not in _TYPE_BY_ALT:
        return None

    id_type = None
    name = record.ID or ""
    parts = name.split(":")
    if len(parts) > 1 and parts[1] in CNV_TYPES:
        id_type = parts[1]

    if id_type is not None:
        alt_implied = ALT_LOH if is_loh_alt else alts[0]
        if ALTERNATE_BY_TYPE[id_type] != alt_implied:
            logger.warning(
                f"CNV record {record.CHROM}:{record.POS} has ID [{name}] typed {id_type} but ALT {list(alts)}; "
                f"trusting the ID."
            )
        return id_type

    if is_loh_alt:
        # Both LOH spellings say only that heterozygosity was lost, never whether the copy number
        # changed with it. Guessing would silently mislabel copy-neutral segments as gains.
        raise ValueError(
            f"CNV record {record.CHROM}:{record.POS} has an LOH ALT {list(alts)} but no parseable "
            f"DRAGEN type in its ID [{name}]: cannot tell {CNLOH} from {GAINLOH}."
        )

    return _TYPE_BY_ALT[alts]


def process_occurrence(
    record: Variant,
    part: int,
    seq_id: int,
    tenant_code: str,
    task_id: int,
    aliquot: str,
    sample_idx: int,
    cnv_type: str,
) -> dict:
    """Processes a somatic CNV occurrence and extracts relevant information for the tumor sample.

    Args:
        record (Variant): A `cyvcf2.Variant` object representing the CNV segment to process.
        part (int): The partition the task belongs to.
        seq_id (int): The sequencing experiment id.
        tenant_code (str): The tenant the experiment belongs to.
        task_id (int): The task that produced the file.
        aliquot (str): The aliquot of the sample being processed.
        sample_idx (int): The index of the sample in the VCF record.
        cnv_type (str): The type resolved by `resolve_cnv_type`, passed in so an unclassifiable
            record can be skipped before a row is ever built.

    Returns:
        dict: A dictionary containing the processed occurrence data.
    """
    rlen = record.INFO.get("REFLEN")
    start = record.POS
    end = record.end
    calls = calls_without_phased(record, sample_idx)

    # `SVLEN` is declared `Number=.` and carries one value per ALT allele, so an LOH record reads
    # `SVLEN=-220050,220050` -- a tuple against a scalar column. The alleles describe the same
    # segment, so the first value is the segment length.
    svlen = record.INFO.get("SVLEN", None)
    if isinstance(svlen, tuple | list):
        svlen = svlen[0] if svlen else None

    occurrence = {
        "part": part,
        "seq_id": seq_id,
        "tenant_code": tenant_code,
        "task_id": task_id,
        "aliquot": aliquot,
        "chromosome": record.CHROM.replace("chr", ""),
        "alternate": ALTERNATE_BY_TYPE[cnv_type],
        "start": start,
        "end": end,
        "type": cnv_type,
        "length": end - start,
        "name": record.ID,
        "quality": float(record.QUAL) if record.QUAL is not None else None,
        "filter": record.FILTER or "PASS",
        "reflen": rlen,
        "svlen": svlen,
        # VCF 4.4 may drop `SVTYPE` entirely; every record in these files is a CNV segment.
        "svtype": record.INFO.get("SVTYPE", "CNV"),
        "ciend": record.INFO.get("CIEND", None),
        "cipos": record.INFO.get("CIPOS", None),
        "bc": format_value(record, "BC", sample_idx, int),
        "pe": format_values(record, "PE", sample_idx, int),
        "sm": format_value(record, "SM", sample_idx, float),
        "calls": calls,
        "phased": record.gt_phases[sample_idx],
        "cn": format_value(record, "CN", sample_idx, int),
        "cnf": format_value(record, "CNF", sample_idx, float),
        "cnq": format_value(record, "CNQ", sample_idx, float),
        "mcn": format_value(record, "MCN", sample_idx, int),
        "mcnf": format_value(record, "MCNF", sample_idx, float),
        "mcnq": format_value(record, "MCNQ", sample_idx, float),
        "maf": format_value(record, "MAF", sample_idx, float),
        "sd": format_value(record, "SD", sample_idx, float),
        "ascn_as": format_value(record, "AS", sample_idx, int),
    }

    return occurrence
