"""
Module: vcf.annotation

This module defines the extended VCF variant annotation schema and a function to process
annotated variant records. It builds on the common VCF schema and adds additional fields
based on transcript annotations such as VEP (Variant Effect Predictor) outputs.

Dependencies:
    - cyvcf2: For handling VCF records.
    - pyiceberg: For defining the Iceberg table schema.
    - vcf.common: For common VCF fields and metadata extraction.
    - iceberg.utils: For merging multiple schemas into one unified Iceberg schema.

Exports:
    - SCHEMA: A merged Iceberg schema including common and annotation-specific fields.
    - process_variant: Function to extract and transform a variant record into a row matching the schema.
"""

from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import BooleanType, IntegerType, ListType, StringType

from radiant.tasks.iceberg.utils import merge_schemas
from radiant.tasks.vcf.common import SCHEMA as COMMON_SCHEMA
from radiant.tasks.vcf.common import Common

# Extended Iceberg schema for annotated variants, merging common fields and annotation-specific fields.
SCHEMA = merge_schemas(
    COMMON_SCHEMA,
    Schema(
        NestedField(301, "variant_class", StringType(), required=False),
        NestedField(302, "symbol", StringType(), required=False),
        NestedField(
            303,
            "consequences",
            ListType(304, element_type=StringType()),
            required=False,
        ),
        NestedField(305, "vep_impact", StringType(), required=False),
        NestedField(306, "impact_score", IntegerType(), required=False),
        NestedField(307, "mane_select", StringType(), required=False),
        NestedField(308, "is_mane_select", BooleanType(), required=False),
        NestedField(309, "is_mane_plus", BooleanType(), required=False),
        NestedField(310, "is_canonical", BooleanType(), required=False),
        NestedField(311, "rsnumber", ListType(312, element_type=StringType()), required=False),
        NestedField(313, "hgvsg", StringType(), required=False),
        NestedField(314, "hgvsc", StringType(), required=False),
        NestedField(315, "hgvsp", StringType(), required=False),
        NestedField(316, "dna_change", StringType(), required=False),
        NestedField(317, "aa_change", StringType(), required=False),
        NestedField(318, "transcript_id", StringType(), required=False),
    ),
)


def process_variant(record: Variant, picked_consequence: dict, common: Common):
    """
    Processes a single VCF variant record with transcript consequence annotations.

    Args:
        record (Variant): A cyvcf2.Variant object representing a variant record from a VCF file.
        picked_consequence (dict): A dictionary containing the most relevant transcript annotation (e.g., VEP output).
        common (Common): A utility object containing common precomputed fields (like position, alleles, etc.).

    Returns:
        dict: A dictionary with keys matching the SCHEMA fields, ready to be written to an Iceberg table.
    """

    variant = {
        "case_id": common.case_id,
        "locus": common.locus,
        "locus_hash": common.locus_hash,
        "chromosome": common.chromosome,
        "start": common.start,
        "end": common.end,
        "reference": common.reference,
        "alternate": common.alternate,
        "rsnumber": record.ID,
    }
    if picked_consequence:
        picked_fields = {
            "variant_class": picked_consequence.get("variant_class"),
            "symbol": picked_consequence.get("symbol"),
            "consequences": picked_consequence.get("consequences"),
            "vep_impact": picked_consequence.get("vep_impact"),
            "impact_score": picked_consequence.get("impact_score"),
            "mane_select": picked_consequence.get("mane_select"),
            "is_mane_select": picked_consequence.get("is_mane_select"),
            "is_mane_plus": picked_consequence.get("is_mane_plus"),
            "is_canonical": picked_consequence.get("is_canonical"),
            "hgvsg": picked_consequence.get("hgvsg"),
            "hgvsp": picked_consequence.get("hgvsp"),
            "hgvsc": picked_consequence.get("hgvsc"),
            "dna_change": picked_consequence.get("dna_change"),
            "aa_change": picked_consequence.get("aa_change"),
            "transcript_id": picked_consequence.get("transcript_id"),
        }

        variant = {**variant, **picked_fields}
    return variant
