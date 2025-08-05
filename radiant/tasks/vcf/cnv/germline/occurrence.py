from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import FloatType, IntegerType, ListType, StringType

SCHEMA: Schema = Schema(
    NestedField(100, "part", IntegerType(), required=True),
    NestedField(101, "seq_id", IntegerType(), required=True),
    NestedField(102, "aliquot", StringType(), required=True),
    NestedField(103, "chromosome", StringType(), required=True),
    NestedField(104, "start", IntegerType(), required=True),
    NestedField(105, "end", IntegerType(), required=True),
    NestedField(106, "type", StringType(), required=True),
    NestedField(107, "length", IntegerType(), required=True),
    NestedField(108, "name", StringType(), required=True),
    NestedField(109, "quality", FloatType(), required=False),
    NestedField(110, "calls", ListType(200, IntegerType()), required=False),
    NestedField(111, "filter", StringType(), required=False),
    NestedField(112, "bc", IntegerType(), required=False),
    NestedField(113, "cn", IntegerType(), required=False),
    NestedField(114, "pe", ListType(201, IntegerType()), required=False),
    NestedField(115, "sm", FloatType(), required=False),
    NestedField(116, "svtype", StringType(), required=False),
    NestedField(117, "svlen", IntegerType(), required=False),
    NestedField(118, "reflen", IntegerType(), required=False),
    NestedField(119, "ciend", ListType(202, IntegerType()), required=False),
    NestedField(120, "cipos", ListType(203, IntegerType()), required=False),
)


def process_occurrence(record: Variant, part: int, seq_id: int, aliquot: str, sample_idx: int) -> dict:
    """
    Processes a cnv occurrence and extracts relevant information for each sample in the pedigree.

    Parameters:
        record (Variant): A `cyvcf2.Variant` object representing the genetic variant to process.

    Returns:
        dict: A dictionary containing the processed occurrence data
        :param record:
        :param sample_idx:
        :param aliquot:
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
        "aliquot": aliquot,
        "chromosome": record.CHROM.replace("chr", ""),
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
        "bc": record.format("BC")[sample_idx][0] if record.format("BC") else None,
        "cn": record.format("CN")[sample_idx][0] if record.format("CN") else None,
        "pe": record.format("PE")[sample_idx].tolist(),
        "sm": record.format("SM")[sample_idx][0] if record.format("SM") else None,
        "calls": record.genotypes[sample_idx][:2],
    }

    return occurrence
