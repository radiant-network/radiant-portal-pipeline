import logging

from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import IntegerType, StringType

from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.snv.common import Common, locus_and_hash

logger = logging.getLogger("airflow.task")

COMPHET_INFO_FIELD = "slivar_comphet"

# Ids shared with the occurrence schema (common.py and occurrence.py) are reused for the same fields.
SCHEMA = Schema(
    NestedField(1, "task_id", IntegerType(), required=True),
    NestedField(3, "locus_hash", StringType(), required=True),
    NestedField(100, "part", IntegerType(), required=True),
    NestedField(101, "seq_id", IntegerType(), required=True),
    NestedField(103, "tenant_code", StringType(), required=True),
    NestedField(300, "partner_locus_hash", StringType(), required=True),
    NestedField(301, "symbol", StringType(), required=True),
)


def process_comphet(record: Variant, ped: Pedigree, common: Common) -> list[dict]:
    """Compound-het pairs of a record, from the slivar INFO tag ``slivar_comphet``.

    Each comma-separated entry reads ``sample/gene/pair_id/chrom/pos/ref/alt``: the sample (VCF aliquot),
    the gene of the pair and the partner variant (1-based POS, see the slivar wiki). Slivar tags both
    variants of a pair, so each pair comes out once per direction. Only the entries of a progeny of the
    task are kept. A malformed entry is logged and skipped.
    """
    value = record.INFO.get(COMPHET_INFO_FIELD)
    if not value:
        return []

    progenies = {exp.aliquot: exp for exp in ped.progenies}
    rows = {}
    for entry in value.split(","):
        fields = entry.split("/")
        if len(fields) != 7 or not fields[1] or not fields[4].isdigit():
            logger.warning(f"Skipped malformed {COMPHET_INFO_FIELD} entry '{entry}' at {common.locus}")
            continue
        sample, symbol, _pair_id, chrom, pos, ref, alt = fields
        exp = progenies.get(sample)
        if exp is None:
            continue
        _, partner_locus_hash = locus_and_hash(chrom.replace("chr", ""), int(pos), ref, alt)
        # Several pair ids can link the same two variants in the same gene, keep one row.
        rows[(exp.seq_id, partner_locus_hash, symbol)] = {
            "task_id": common.task_id,
            "locus_hash": common.locus_hash,
            "part": common.part,
            "seq_id": exp.seq_id,
            "tenant_code": exp.tenant_code,
            "partner_locus_hash": partner_locus_hash,
            "symbol": symbol,
        }
    return list(rows.values())
