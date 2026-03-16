import hashlib
from dataclasses import dataclass

from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import IntegerType, StringType


@dataclass()
class Common:
    """
    Represents common genomic variant information shared across different variant processing steps.

    Attributes:
        task_id (int): Identifier for the task or sample the variant belongs to.
        locus (str): Unique string representation of the variant position and alleles,
            typically in the format 'chrom-start-ref-alt'.
        locus_hash (str): Placeholder for a hash value uniquely identifying the locus.
            This can be used for quick comparisons or joins.
        chromosome (str): Chromosome identifier, e.g., '1', 'X', 'MT'.
        start (int): 1-based position where the variant starts (VCF-style POS field).
        end (int): End position of the variant, usually the same as start for SNVs.
        reference (str): Reference allele observed at the variant position.
        alternate (str): Alternate allele observed at the variant position.
    """

    task_id: int
    part: int
    locus: str
    locus_hash: str
    chromosome: str
    start: int
    end: int
    reference: str
    alternate: str


def process_common(record: Variant, task_id: int, part: int) -> Common:
    chrom = record.CHROM.replace("chr", "")
    pos = record.POS
    ref = record.REF
    alt = record.ALT[0]
    info_end = record.end
    locus = f"{chrom}-{pos}-{ref}-{alt}"
    locus_hash = hashlib.sha256(locus.encode()).hexdigest()
    return Common(
        task_id=task_id,
        part=part,
        locus=locus,
        locus_hash=locus_hash,
        chromosome=chrom,
        start=pos,
        end=info_end,
        reference=ref,
        alternate=alt,
    )


SCHEMA = Schema(
    NestedField(1, "task_id", IntegerType(), required=True),
    NestedField(2, "locus", StringType(), required=True),
    NestedField(3, "locus_hash", StringType(), required=True),
    NestedField(4, "chromosome", StringType(), required=True),
    NestedField(5, "start", IntegerType(), required=True),
    NestedField(6, "end", IntegerType(), required=False),
    NestedField(7, "reference", StringType(), required=True),
    NestedField(8, "alternate", StringType(), required=True),
)
