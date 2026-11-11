from urllib.parse import unquote

from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, StringType

from radiant.tasks.vcf.snv.germline.inheritance import slivar_parental_origin
from radiant.tasks.vcf.vcf_utils import calls_without_phased, format_array, format_value, format_values

SCHEMA: Schema = Schema(
    NestedField(100, "part", IntegerType(), required=True),
    NestedField(101, "seq_id", IntegerType(), required=True),
    NestedField(102, "tenant_code", StringType(), required=True),
    NestedField(103, "task_id", IntegerType(), required=True),
    NestedField(104, "aliquot", StringType(), required=True),
    NestedField(105, "chromosome", StringType(), required=True),
    NestedField(106, "alternate", StringType(), required=True),
    NestedField(107, "start", IntegerType(), required=True),
    NestedField(108, "end", IntegerType(), required=True),
    NestedField(109, "type", StringType(), required=True),
    NestedField(110, "length", IntegerType(), required=True),
    NestedField(111, "name", StringType(), required=True),
    NestedField(112, "quality", FloatType(), required=False),
    NestedField(113, "calls", ListType(200, IntegerType()), required=False),
    NestedField(114, "filter", StringType(), required=False),
    NestedField(115, "bc", IntegerType(), required=False),
    NestedField(116, "cn", IntegerType(), required=False),
    # `pe`, `mother_pe`, `father_pe`: NULL (a sample with no call of its own, or no PE), or every value present.
    # `format_values` rejects a partly missing vector.
    NestedField(117, "pe", ListType(201, IntegerType()), required=False),
    NestedField(118, "sm", FloatType(), required=False),
    NestedField(119, "svtype", StringType(), required=False),
    NestedField(120, "svlen", IntegerType(), required=False),
    NestedField(121, "reflen", IntegerType(), required=False),
    NestedField(122, "ciend", ListType(202, IntegerType()), required=False),
    NestedField(123, "cipos", ListType(203, IntegerType()), required=False),
    NestedField(124, "phased", BooleanType(), required=False),
    # Filled on the proband's rows of a `radiant_germline_cnv_annotation` task only (the slivar VCF).
    # Last, in the StarRocks table's order: the occurrence insert is positional.
    NestedField(125, "parental_origin", StringType(), required=False),
    NestedField(126, "transmission_mode", StringType(), required=False),
    NestedField(127, "mother_calls", ListType(204, IntegerType()), required=False),
    NestedField(128, "mother_cn", IntegerType(), required=False),
    NestedField(129, "mother_bc", IntegerType(), required=False),
    NestedField(130, "mother_pe", ListType(205, IntegerType()), required=False),
    NestedField(131, "mother_sm", FloatType(), required=False),
    NestedField(132, "mother_name", StringType(), required=False),
    NestedField(133, "father_calls", ListType(206, IntegerType()), required=False),
    NestedField(134, "father_cn", IntegerType(), required=False),
    NestedField(135, "father_bc", IntegerType(), required=False),
    NestedField(136, "father_pe", ListType(207, IntegerType()), required=False),
    NestedField(137, "father_sm", FloatType(), required=False),
    NestedField(138, "father_name", StringType(), required=False),
    # The VCF record's `ID`. On the slivar set it can differ from `name` (the proband's `FORMAT/OID`): it is the call
    # truvari kept for the cluster. When cnv-post-processing runs Exomiser on the family VCF (no proband VCF), the
    # report's `VCF_ID` is this ID; on the proband VCF it is `name`. The tenant `exomiser_cnv` insert joins
    # `raw_exomiser_cnv` on `name`, then on this column (RAD-181). Iceberg only: the StarRocks table does not carry it.
    NestedField(139, "vcf_id", StringType(), required=False),
)

# Any parental origin, or none: `unknown_cn` is called before the rule table is read (cnv_moi step 3).
ANY_ORIGIN = None

# The CNV mode tags of cnv-post-processing (`--family-expr` in conf/modules.config, the list in docs/output.md), each
# with the parental origins (`po_*` values, as PO_TAGS maps them) its TRIO_TABLE rows give the same child
# (assets/cnv-slivar-functions.js @ c9e7768). One row gives a child both its mode call and its parent of origin, so
# they never disagree for that child. `None` in a set: the catch-all row, which has no origin. The X-linked names are
# renamed `denovo` and `recessive` calls (X_LINKED_CALLS), and keep those rows' origins.
#
# Each tag is stored under its own name: the CNV rules read copy numbers, not dominance, so none of them is an SNV
# mode, and an uncertain tag (`denovo_suspect`, `possible_*`, `recessive_lowconf`) never becomes a confirmed one.
CNV_MODE_ORIGINS: dict[str, frozenset[str | None] | None] = {
    "denovo": frozenset({"DENOVO"}),
    "denovo_suspect": frozenset({"DENOVO"}),
    "possible_denovo": frozenset({"POSSIBLE_DENOVO"}),
    "x_linked_denovo": frozenset({"DENOVO"}),
    "inherited_maternal": frozenset({"MOTHER"}),
    "inherited_paternal": frozenset({"FATHER"}),
    "possible_inherited_maternal": frozenset({"POSSIBLE_MOTHER"}),
    "possible_inherited_paternal": frozenset({"POSSIBLE_FATHER"}),
    "recessive": frozenset({"BOTH"}),
    "recessive_lowconf": frozenset({"MOTHER", "FATHER"}),
    "x_linked_recessive": frozenset({"BOTH"}),
    "inheritance_unresolved": frozenset({"BOTH", "AMBIGUOUS", None}),
    "mendelian_inconsistent": frozenset({None}),
    "unknown_cn": ANY_ORIGIN,
}
CNV_MODE_TAGS = tuple(CNV_MODE_ORIGINS)

# Each sample's DRAGEN record ID, written by cnv-post-processing before the truvari collapses (BIOINFO-214), with
# `:` percent-encoded as `%3A`: FORMAT values are `:`-separated, and DRAGEN IDs contain it.
ORIGINAL_ID_FORMAT = "OID"


def sample_format(record: Variant, sample_idx: int) -> dict:
    """The DRAGEN FORMAT fields of one sample: `GT`, `CN`, `BC`, `PE` and `SM`. A missing value is None."""
    return {
        "calls": calls_without_phased(record, sample_idx),
        "cn": format_value(record, "CN", sample_idx, int),
        "bc": format_value(record, "BC", sample_idx, int),
        "pe": format_values(record, "PE", sample_idx, int),
        "sm": format_value(record, "SM", sample_idx, float),
    }


def original_id(record: Variant, sample_idx: int) -> str | None:
    """The sample's own DRAGEN record ID, from `FORMAT/OID`. None when the VCF predates it (BIOINFO-214, the header
    does not declare it) or the sample has none."""
    values = format_array(record, ORIGINAL_ID_FORMAT)
    if values is None:
        return None
    value = str(values[sample_idx])
    return None if value in ("", ".") else unquote(value)


def is_carrier(calls: list[int]) -> bool:
    """At least one ALT allele: `0/1`, `1/1`, `./1` or a haploid `1`. `0/0`, `./.` and `0/.` are not."""
    return any(c > 0 for c in calls)


def slivar_cnv_transmission_mode(record: Variant, aliquot: str, parental_origin: str | None) -> str | None:
    """The proband's mode tag, or None when it cannot be told apart.

    slivar values a mode tag with the family's affected members, not with the child whose call gave it: with an
    affected sibling, the proband can be listed under the sibling's tag too. A tag whose rule-table origins do not
    include the proband's own `po_*` value comes from another child and is dropped. When more than one tag remains,
    none is guessed: the mode stays NULL.
    """
    candidates = []
    for tag, origins in CNV_MODE_ORIGINS.items():
        value = record.INFO.get(tag)
        if value is None or aliquot not in str(value).split(","):
            continue
        if origins is ANY_ORIGIN or parental_origin in origins:
            candidates.append(tag)
    return candidates[0] if len(candidates) == 1 else None


def process_occurrence(
    record: Variant, part: int, seq_id: int, tenant_code: str, task_id: int, aliquot: str, sample_idx: int
) -> dict:
    """
    Processes a cnv occurrence and extracts relevant information for each sample in the pedigree.

    Parameters:
        record (Variant): A `cyvcf2.Variant` object representing the genetic variant to process.

    Returns:
        dict: A dictionary containing the processed occurrence data
        :param record:
        :param sample_idx:
        :param aliquot:
        :param task_id:
        :param seq_id:
        :param part:
    """

    cnv_type = "GAIN" if record.ALT[0] == "<DUP>" else "LOSS" if record.ALT[0] == "<DEL>" else "UNKNOWN"

    rlen = record.INFO.get("REFLEN")
    start = record.POS
    end = record.end
    occurrence = {
        "part": part,
        "seq_id": seq_id,
        "tenant_code": tenant_code,
        "task_id": task_id,
        "aliquot": aliquot,
        "chromosome": record.CHROM.replace("chr", ""),
        "alternate": record.ALT[0],
        "start": start,
        "end": end,
        "type": cnv_type,
        "length": end - start,
        "name": record.ID,
        "vcf_id": record.ID,
        "quality": int(record.QUAL) if record.QUAL is not None else None,
        "filter": record.FILTER or "PASS",
        "reflen": rlen,
        "svlen": record.INFO.get("SVLEN", None),
        "svtype": record.INFO.get("SVTYPE", None),
        "ciend": record.INFO.get("CIEND", None),
        "cipos": record.INFO.get("CIPOS", None),
        **sample_format(record, sample_idx),
        "phased": record.gt_phases[sample_idx],
    }

    return occurrence


def process_slivar_occurrence(
    record: Variant,
    part: int,
    seq_id: int,
    tenant_code: str,
    task_id: int,
    aliquot: str,
    sample_idx: int,
    mother_idx: int | None,
    father_idx: int | None,
) -> dict:
    """The proband's occurrence of one slivar record: its DRAGEN fields, the parental origin and the
    transmission mode slivar gave it, and each parent's FORMAT values of the same record.

    `name` is the proband's own DRAGEN call ID (`FORMAT/OID`), equal to `name` on its alignment-task row. The
    record's `ID`, like its `POS` and `END`, is the call truvari kept for the cluster, often a parent's; it stays
    the name only when the VCF has no `OID` (made before BIOINFO-214).

    `mother_idx` / `father_idx` are the parents' indexes in the record, None when the parent is not in the VCF: its
    columns then stay None, as do the origin and mode columns of a record with no tag listing the proband.
    """
    occurrence = process_occurrence(record, part, seq_id, tenant_code, task_id, aliquot, sample_idx)
    occurrence["name"] = original_id(record, sample_idx) or record.ID
    occurrence["parental_origin"] = slivar_parental_origin(record, aliquot)
    occurrence["transmission_mode"] = slivar_cnv_transmission_mode(record, aliquot, occurrence["parental_origin"])
    for role, idx in (("mother", mother_idx), ("father", father_idx)):
        parent = sample_format(record, idx) if idx is not None else {}
        for field in ("calls", "cn", "bc", "pe", "sm"):
            occurrence[f"{role}_{field}"] = parent.get(field)
        occurrence[f"{role}_name"] = original_id(record, idx) if idx is not None else None
    return occurrence
