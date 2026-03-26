from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, StringType

from radiant.tasks.vcf.vcf_utils import calls_without_phased

SCHEMA: Schema = Schema(
    NestedField(100, "part", IntegerType(), required=True),
    NestedField(101, "seq_id", IntegerType(), required=True),
    NestedField(102, "task_id", IntegerType(), required=True),
    NestedField(103, "aliquot", StringType(), required=True),
    NestedField(104, "chromosome", StringType(), required=True),
    NestedField(105, "alternate", StringType(), required=True),
    NestedField(106, "start", IntegerType(), required=True),
    NestedField(107, "end", IntegerType(), required=True),
    NestedField(108, "type", StringType(), required=True),
    NestedField(109, "length", IntegerType(), required=True),
    NestedField(110, "name", StringType(), required=True),
    NestedField(111, "quality", FloatType(), required=False),
    NestedField(112, "calls", ListType(200, IntegerType()), required=False),
    NestedField(113, "filter", StringType(), required=False),
    NestedField(114, "bc", IntegerType(), required=False),
    NestedField(115, "cn", IntegerType(), required=False),
    NestedField(116, "pe", ListType(201, IntegerType()), required=False),
    NestedField(117, "sm", FloatType(), required=False),
    NestedField(118, "svtype", StringType(), required=False),
    NestedField(119, "svlen", IntegerType(), required=False),
    NestedField(120, "reflen", IntegerType(), required=False),
    NestedField(121, "ciend", ListType(202, IntegerType()), required=False),
    NestedField(122, "cipos", ListType(203, IntegerType()), required=False),
    NestedField(123, "phased", BooleanType(), required=False),
)


def process_occurrence(record: Variant, part: int, seq_id: int, task_id: int, aliquot: str, sample_idx: int) -> dict:
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
    calls = calls_without_phased(record, sample_idx)
    occurrence = {
        "part": part,
        "seq_id": seq_id,
        "task_id": task_id,
        "aliquot": aliquot,
        "chromosome": record.CHROM.replace("chr", ""),
        "alternate": record.ALT[0],
        "start": start,
        "end": end,
        "type": cnv_type,
        "length": end - start,
        "name": record.ID,
        "quality": int(record.QUAL) if record.QUAL is not None else None,
        "filter": record.FILTER or "PASS",
        "reflen": rlen,
        "svlen": record.INFO.get("SVLEN", None),
        "svtype": record.INFO.get("SVTYPE", None),
        "ciend": record.INFO.get("CIEND", None),
        "cipos": record.INFO.get("CIPOS", None),
        "bc": record.format("BC")[sample_idx][0] if record.format("BC") is not None else None,
        "cn": record.format("CN")[sample_idx][0] if record.format("CN") is not None else None,
        "pe": record.format("PE")[sample_idx].tolist() if record.format("PE") is not None else None,
        "sm": record.format("SM")[sample_idx][0] if record.format("SM") is not None else None,
        "calls": calls,
        "phased": record.gt_phases[sample_idx],
    }

    return occurrence
