"""
Module for processing variant consequence annotations (VEP CSQ) from VCF records
and transforming them into a structured format compatible with Iceberg tables.

This module defines:
- A schema for consequence data.
- A helper dataclass for exon rank/total.
- Functions to parse CSQ headers, extract CSQ field values, and process consequences.
- Resolution of the transcript catalogue (Ensembl or RefSeq) each annotation came from,
  which is what lets a VEP `--merged` file be told apart from a single-catalogue one.
- The MANE flags, plus version-free copies of the transcript identifier and of the MANE
  cross-reference, which together bridge a RefSeq annotation to its Ensembl twin.

Dependencies:
- cyvcf2: for reading VCF records.
- pyiceberg: for defining the Iceberg schema.
- Common metadata and schema merging from internal modules.
"""

import logging
import re
from collections import Counter

from cyvcf2 import Variant
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import BooleanType, IntegerType, ListType, StringType, StructType

from radiant.tasks.iceberg.utils import merge_schemas
from radiant.tasks.vcf.snv.common import SCHEMA as COMMON_SCHEMA
from radiant.tasks.vcf.snv.common import Common

logger = logging.getLogger("airflow.task")

CSQ_FORMAT_FIELD = "CSQ"

# The two transcript catalogues a VEP `--merged` file annotates against. Spelled as VEP
# spells them in its `SOURCE` column, since that value is stored as is.
SOURCE_ENSEMBL = "Ensembl"
SOURCE_REFSEQ = "RefSeq"

# The two values VEP's `MANE` column takes, spelled as VEP spells them. Note that `MANE` and
# `MANE_SELECT` are unrelated columns despite the near-identical names: `MANE` is the flag
# resolved here, while `MANE_SELECT` holds an accession (see `strip_transcript_version`).
# The two labels are mutually exclusive on a given transcript -- MANE Plus Clinical is an
# *additional* transcript for genes the Select one does not cover well enough for clinical
# reporting, so a gene can carry both, on two different transcripts.
MANE_SELECT_FLAG = "MANE_Select"
MANE_PLUS_CLINICAL_FLAG = "MANE_Plus_Clinical"

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
        NestedField(224, "mane_pair_transcript_id", StringType(), required=False),
        NestedField(225, "transcript_id_unversioned", StringType(), required=False),
    ),
)


def get_csq_field(csq_fields, fields, field_name):
    """
    Safely retrieves a field from CSQ annotation by name, ignoring case.

    VEP does not spell its CSQ column names consistently across versions and plugins
    (`SOURCE` vs `Source`), and a lookup under the wrong spelling silently yields None.
    Matching on the lowercased name removes that failure mode.

    Args:
        csq_fields (dict): Mapping of lowercased CSQ field names to indexes, as
            returned by `parse_csq_header`.
        fields (list): Parsed CSQ annotation fields from a single transcript.
        field_name (str): Name of the CSQ field to retrieve, in any case.

    Returns:
        str or None: Value of the CSQ field or None if not found.
    """
    index = csq_fields.get(field_name.lower())
    return fields[index] if index is not None else None


def strip_transcript_version(value: str | None) -> str | None:
    """
    Drops the version suffix from a transcript accession, so it can be used as a join key.

    Two columns need this, and for different reasons -- which is why it is applied to both
    sides of the MANE pair rather than only to the cross-reference:

    - `MANE_SELECT` is a pointer to the paired transcript in the *other* catalogue, and it is
      always versioned (`ENST00000269305.9`, `NM_000546.6`).
    - `Feature` is versioned on RefSeq rows (`NM_000546.6`) but *not* on Ensembl ones
      (`ENST00000269305`). Measured on a reference `--merged --mane` file: stripping only the
      cross-reference joins RefSeq rows onto their Ensembl twin 99.9% of the time but Ensembl
      rows onto their RefSeq twin 0% of the time. Stripping both sides makes it 100% either
      way, and is also what lines the value up with dbNSFP's and gnomAD constraint's
      unversioned Ensembl transcript ids.

    Args:
        value (str | None): A transcript accession, versioned or not.

    Returns:
        str or None: The accession up to the first `.`, or the input unchanged when it is
            None or empty. An absent CSQ column reads as None and a declared-but-empty one as
            "", and callers rely on that distinction surviving, so neither is collapsed.
    """
    return value.split(".")[0] if value else value


# Both catalogues use disjoint identifier namespaces, so the identifier alone is enough to
# tell them apart. Every pattern is anchored: unanchored matching would misfire on gene
# symbols, some of which contain these substrings.
_ENSEMBL_FEATURE_RE = re.compile(r"^(?:ENST|ENSR|LRG_)")
_REFSEQ_FEATURE_RE = re.compile(r"^[NX][MRP]_")
_ENSEMBL_GENE_RE = re.compile(r"^ENSG\d+$")
_REFSEQ_GENE_RE = re.compile(r"^\d+$")

_KNOWN_SOURCES = {SOURCE_ENSEMBL.lower(): SOURCE_ENSEMBL, SOURCE_REFSEQ.lower(): SOURCE_REFSEQ}

# A merged file carries one SOURCE value per annotation block, so warning per row would emit
# one line per block. Warn once per distinct unexpected value per process instead.
_warned_unknown_sources: set[str] = set()


def resolve_source(source: str | None, transcript_id: str | None, gene_id: str | None) -> str | None:
    """
    Resolves which transcript catalogue an annotation block came from.

    VEP `--merged` files annotate against both Ensembl and RefSeq and carry an explicit
    `SOURCE` column; older single-catalogue files carry no such column. This resolves both
    file types through one path, falling back on the identifier namespaces when VEP does not
    state the source itself.

    Rules, first one that answers wins:

    1. `SOURCE` from the CSQ block, matched case-insensitively. This is VEP's own answer.
    2. The transcript (`Feature`) prefix — `ENST` / `ENSR` (regulatory) / `LRG_` are Ensembl,
       `NM_ / NR_ / NP_ / XM_ / XR_ / XP_` are RefSeq.
    3. The gene (`Gene`) format — `ENSG…` is Ensembl, a bare NCBI numeric id is RefSeq.
    4. Nothing. Intergenic blocks have neither a gene nor a transcript and belong to no
       catalogue, so they resolve to None rather than being forced into one.

    Rules 2 and 3 are both needed because they fail on opposite rows: a regulatory feature
    has an `ENSR…` transcript and an *empty* gene, while a block with a gene but no feature
    id can only be resolved by its gene.

    An unexpected `SOURCE` value falls through to rules 2-3 rather than being stored, keeping
    the column a closed set of `Ensembl` / `RefSeq` / None.

    Args:
        source (str | None): Raw `SOURCE` value, or None when the column is absent.
        transcript_id (str | None): Raw `Feature` value, or None when the column is absent.
        gene_id (str | None): Raw `Gene` value, or None when the column is absent.

    Returns:
        str or None: `SOURCE_ENSEMBL`, `SOURCE_REFSEQ`, or None when no rule applies.
    """
    # A column absent from the CSQ header reads as None, one declared but empty for this
    # block reads as "". Both mean "no value here" and must fall through to the next rule.
    source = (source or "").strip()
    transcript_id = (transcript_id or "").strip()
    gene_id = (gene_id or "").strip()

    if source:
        known = _KNOWN_SOURCES.get(source.lower())
        if known:
            return known
        if source not in _warned_unknown_sources:
            _warned_unknown_sources.add(source)
            logger.warning(
                f"Unexpected CSQ SOURCE value {source!r}: not one of "
                f"{SOURCE_ENSEMBL!r} / {SOURCE_REFSEQ!r}. Falling back to the transcript and "
                f"gene identifiers to resolve the source."
            )

    if transcript_id:
        if _ENSEMBL_FEATURE_RE.match(transcript_id):
            return SOURCE_ENSEMBL
        if _REFSEQ_FEATURE_RE.match(transcript_id):
            return SOURCE_REFSEQ

    if gene_id:
        if _ENSEMBL_GENE_RE.match(gene_id):
            return SOURCE_ENSEMBL
        if _REFSEQ_GENE_RE.match(gene_id):
            return SOURCE_REFSEQ

    return None


def process_consequence(record: Variant, csq_fields: dict[str, int], common: Common) -> tuple[dict, list[dict]]:
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
            exon_field = get_csq_field(csq_fields, fields, "EXON")
            exon = exon_field.split("/") if exon_field else []
            vep_impact = get_csq_field(csq_fields, fields, "IMPACT")
            hgvsg = get_csq_field(csq_fields, fields, "HGVSg")
            hgvsp = get_csq_field(csq_fields, fields, "HGVSp")
            hgvsc = get_csq_field(csq_fields, fields, "HGVSc")
            picked = get_csq_field(csq_fields, fields, "PICK") == "1"
            transcript_id = get_csq_field(csq_fields, fields, "Feature")
            # `Gene` is read only to resolve the source; it is not part of the schema.
            gene_id = get_csq_field(csq_fields, fields, "Gene")
            # `MANE` carries the flag (`MANE_Select` / `MANE_Plus_Clinical`), `MANE_SELECT`
            # carries the paired transcript in the other catalogue. Deliberately kept
            # independent: files that emit `MANE_SELECT` but no `MANE` column still yield a
            # usable pair, which is what the score borrowing downstream joins on.
            mane = get_csq_field(csq_fields, fields, "MANE")
            mane_select = get_csq_field(csq_fields, fields, "MANE_SELECT")
            consequence = {
                "task_id": common.task_id,
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
                "transcript_id": transcript_id,
                "transcript_id_unversioned": strip_transcript_version(transcript_id),
                "source": resolve_source(get_csq_field(csq_fields, fields, "SOURCE"), transcript_id, gene_id),
                "biotype": get_csq_field(csq_fields, fields, "BIOTYPE"),
                "strand": get_csq_field(csq_fields, fields, "STRAND"),
                "exon": {"rank": str(exon[0]), "total": str(exon[1])} if len(exon) == 2 else None,
                "vep_impact": vep_impact,
                "consequences": get_csq_field(csq_fields, fields, "Consequence").split("&"),
                "mane_select": mane_select,
                # Null on MANE Plus Clinical rows: VEP only fills `MANE_SELECT` on the Select
                # transcript, so those rows have no 1:1 pointer to borrow scores through.
                "mane_pair_transcript_id": strip_transcript_version(mane_select),
                "is_mane_select": mane == MANE_SELECT_FLAG,
                "is_mane_plus": mane == MANE_PLUS_CLINICAL_FLAG,
                "is_picked": picked,
                "is_canonical": get_csq_field(csq_fields, fields, "CANONICAL") == "YES",
                "aa_change": hgvsp.split(":")[-1] if hgvsp else None,
                "dna_change": hgvsc.split(":")[-1] if hgvsc else None,
                "impact_score": IMPACT_SCORE.get(vep_impact, 0),
            }
            if picked:
                pick_consequence = consequence
            consequences.append(consequence)
    if pick_consequence is None:
        pick_consequence = next((c for c in consequences if c["is_canonical"]), None)
    return pick_consequence, consequences


def log_source_counts(source_counts: Counter, task_id: int, vcf_filepath: str) -> None:
    """
    Reports the per-source consequence counts accumulated over one VCF file.

    Two things are reported. The breakdown itself, because a merged file should come out
    roughly half Ensembl and half RefSeq and a large skew means the `--merged` annotation was
    lost upstream. And the count of blocks no rule could classify — those are legitimate
    (intergenic annotations belong to no catalogue) but they are loaded with a null source, so
    they are surfaced rather than left to be discovered as unexplained nulls downstream.

    The totals cover the blocks actually ingested. Multi-allelic records and unsupported
    chromosomes are skipped before extraction, so this is deliberately not the raw annotation
    block count of the file.

    Args:
        source_counts (Counter): Counts keyed by resolved source, including None.
        task_id (int): Id of the annotation task being processed, for log correlation.
        vcf_filepath (str): Path of the VCF file being processed.
    """
    counts = Counter(source_counts)
    unclassified = counts.pop(None, 0)
    breakdown = ", ".join(f"{source}={count}" for source, count in sorted(counts.items())) or "none"
    logger.info(f"Consequence sources for task {task_id}: {breakdown}, unclassified={unclassified}")
    if unclassified:
        logger.warning(
            f"⚠️ Task {task_id}: {unclassified} consequence rows in {vcf_filepath} have no resolvable "
            f"transcript source (intergenic annotations, or identifiers matching neither catalogue). "
            f"They are loaded with a null source, not dropped."
        )


def parse_csq_header(vcf):
    """
    Parses the CSQ header from a VCF and extracts field name to index mapping.

    Args:
        vcf: A cyvcf2.VCF reader object.

    Returns:
        dict[str, int]: Mapping from lowercased CSQ field names to their indexes.
    """
    info_csq = vcf.get_header_type(CSQ_FORMAT_FIELD)
    csq_meta = info_csq.get("Description", "")
    csq_meta = csq_meta.split("Format:")[-1].strip(' "')
    csq_fields = csq_meta.split("|") if csq_meta else []
    return {f.lower(): i for i, f in enumerate(csq_fields)}


IMPACT_SCORE = {"HIGH": 4, "MODERATE": 3, "LOW": 2, "MODIFIER": 1}
