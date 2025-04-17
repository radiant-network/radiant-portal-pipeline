"""
Module for processing variant consequence annotations (VEP CSQ) from VCF records
and transforming them into a structured format compatible with Iceberg tables.

This module defines:
- A schema for consequence data.
- A helper dataclass for exon rank/total.
- Functions to parse CSQ headers, extract CSQ field values, and process consequences.

Dependencies:
- cyvcf2: for reading VCF records.
- pyiceberg: for defining the Iceberg schema.
- Common metadata and schema merging from internal modules.
"""

from typing import List

from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import BooleanType
from pyiceberg.types import IntegerType
from pyiceberg.types import ListType
from pyiceberg.types import StringType
from pyiceberg.types import StructType

from tasks.iceberg.utils import merge_schemas
from tasks.vcf.common import Common
from tasks.vcf.common import SCHEMA as COMMON_SCHEMA

CSQ_FORMAT_FIELD = "CSQ"

# Iceberg schema definition for the consequence annotations,
# merged with a common schema shared across VCF processors.
SCHEMA = merge_schemas(
    COMMON_SCHEMA,
    Schema(
        NestedField(201, "variant_class", StringType(), required=False),
        NestedField(202, "hgvsg", StringType(), required=False),
        NestedField(203, "hgvsp", StringType(), required=False),
        NestedField(204, "hgvsc", StringType(), required=False),
        NestedField(205, "symbol", StringType(), required=False),
        NestedField(206, "transcript_id", StringType(), required=False),
        NestedField(207, "source", StringType(), required=False),
        NestedField(208, "biotype", StringType(), required=False),
        NestedField(209, "strand", StringType(), required=False),
        NestedField(
            210,
            "exon",
            StructType(
                NestedField(211, "rank", StringType(), required=False),
                NestedField(212, "total", StringType(), required=False),
            ),
            required=False,
        ),
        NestedField(213, "vep_impact", StringType(), required=False),
        NestedField(
            214,
            "consequences",
            ListType(215, element_type=StringType()),
            required=False,
        ),
        NestedField(216, "mane_select", StringType(), required=False),
        NestedField(217, "is_mane_select", BooleanType(), required=True),
        NestedField(218, "is_mane_plus", BooleanType(), required=True),
        NestedField(219, "is_picked", BooleanType(), required=True),
        NestedField(220, "is_canonical", BooleanType(), required=True),
        NestedField(221, "aa_change", StringType(), required=False),
        NestedField(222, "dna_change", StringType(), required=False),
        NestedField(223, "impact_score", IntegerType(), required=True),
    ),
)


def get_csq_field(csq_fields, fields, field_name):
    """
    Safely retrieves a field from CSQ annotation by name.

    Args:
        csq_fields (dict): Mapping of CSQ field names to indexes.
        fields (list): Parsed CSQ annotation fields from a single transcript.
        field_name (str): Name of the CSQ field to retrieve.

    Returns:
        str or None: Value of the CSQ field or None if not found.
    """
    return fields[csq_fields[field_name]] if field_name in csq_fields else None


def process_consequence(
    record: Variant, csq_fields: dict[str, int], common: Common
) -> tuple[dict, List[dict]]:
    """
    Processes VEP CSQ annotations from a VCF record and builds structured consequence data.

    Args:
        record (Variant): A cyvcf2 Variant object.
        csq_fields (dict[str, int]): Field name to index mapping from CSQ header.
        common (Common): Shared metadata (e.g. position, allele info).

    Returns:
        tuple:
            - dict: The primary (picked or canonical) consequence.
            - list of dict: All consequence entries for the variant.
    """
    csq = record.INFO.get(CSQ_FORMAT_FIELD, None)
    consequences = []
    pick_consequence = None
    if csq:
        csq_data = csq.split(",")
        for c in csq_data:
            fields = c.split("|")
            exon = get_csq_field(csq_fields, fields, "EXON").split("/")
            vep_impact = get_csq_field(csq_fields, fields, "IMPACT")
            hgvsg = get_csq_field(csq_fields, fields, "HGVSg")
            hgvsp = get_csq_field(csq_fields, fields, "HGVSp")
            hgvsc = get_csq_field(csq_fields, fields, "HGVSc")
            picked = get_csq_field(csq_fields, fields, "PICK") == "1"
            consequence = {
                "case_id": common.case_id,
                "locus": common.locus,
                "locus_hash": common.locus_hash,
                "chromosome": common.chromosome,
                "start": common.start,
                "end": common.end,
                "reference": common.reference,
                "alternate": common.alternate,
                "variant_class": get_csq_field(csq_fields, fields, "VARIANT_CLASS"),
                "hgvsg": hgvsg,
                "hgvsp": hgvsp,
                "hgvsc": hgvsc,
                "symbol": get_csq_field(csq_fields, fields, "SYMBOL"),
                "transcript_id": get_csq_field(csq_fields, fields, "Feature"),
                "source": get_csq_field(csq_fields, fields, "Source"),
                "biotype": get_csq_field(csq_fields, fields, "BIOTYPE"),
                "strand": get_csq_field(csq_fields, fields, "STRAND"),
                "exon": {"rank": exon[0], "total": exon[1]} if len(exon) == 2 else None,
                "vep_impact": vep_impact,
                "consequences": get_csq_field(csq_fields, fields, "Consequence").split(
                    ";"
                ),
                "mane_select": get_csq_field(csq_fields, fields, "ManeSelect"),
                "is_mane_select": False,
                "is_mane_plus": False,
                "is_picked": picked,
                "is_canonical": get_csq_field(csq_fields, fields, "CANONICAL") == "YES",
                "aa_change": hgvsp.split(":")[-1] if hgvsp else None,
                "dna_change": hgvsc.split(":")[-1] if hgvsp else None,
                "impact_score": IMPACT_SCORE.get(vep_impact, 0),
            }
            if picked:
                pick_consequence = consequence
            consequences.append(consequence)
    if pick_consequence is None:
        pick_consequence = next((c for c in consequences if c["is_canonical"]), None)
    return pick_consequence, consequences


def parse_csq_header(vcf):
    """
    Parses the CSQ header from a VCF and extracts field name to index mapping.

    Args:
        vcf: A cyvcf2.VCF reader object.

    Returns:
        dict[str, int]: Mapping from CSQ field names to their indexes.
    """
    info_csq = vcf.get_header_type(CSQ_FORMAT_FIELD)
    csq_meta = info_csq.get("Description", "")
    csq_meta = csq_meta.split("Format:")[-1].strip(' "')
    csq_fields = csq_meta.split("|") if csq_meta else []
    return {f: i for i, f in enumerate(csq_fields)}


IMPACT_SCORE = {"HIGH": 1, "MODERATE": 2, "LOW": 3, "MODIFIER": 4}
