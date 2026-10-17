"""Parental origin and transmission mode read from the slivar tags of the germline VCF (RAD-93).

Since snv-post-processing BIOINFO-239/240, slivar writes them as INFO tags, each listing the samples (VCF aliquots)
the rule holds for: the ``po_*`` tags carry the parental origin, and the mode tags are named after the
``transmission_mode`` values. A ``--family-expr`` tag lists the affected family members, or the family id when none is
affected: a value that is not an aliquot of the task is simply never matched.
"""

from typing import NamedTuple

from cyvcf2 import VCF, Variant

PO_TAGS = {
    "po_denovo": "DENOVO",
    "po_mother": "MOTHER",
    "po_father": "FATHER",
    "po_both": "BOTH",
    "po_ambiguous": "AMBIGUOUS",
    "po_possible_denovo": "POSSIBLE_DENOVO",
    "po_possible_mother": "POSSIBLE_MOTHER",
    "po_possible_father": "POSSIBLE_FATHER",
    "po_unknown": "UNKNOWN",
}

# When several modes list a sample, the first one is kept and the others go to other_transmission_modes.
MODE_ORDER = (
    "autosomal_dominant_de_novo",
    "x_linked_dominant_de_novo",
    "x_linked_recessive_de_novo",
    "autosomal_recessive",
    "x_linked_recessive",
    "autosomal_dominant",
    "x_linked_dominant",
    "uniparental_disomy",
)

# Why no mode can be given; the pipeline makes them exclusive, and any mode wins over them.
GENOTYPE_TAGS = (
    "unknown_parents_genotype",
    "unknown_father_genotype",
    "unknown_mother_genotype",
    "unknown_proband_genotype",
    "non_carrier_proband",
)

CANDIDATE_TAG = "candidate"


class SlivarMethods(NamedTuple):
    """Which field a VCF takes from slivar. The two are decided independently from its header."""

    parental_origin: bool = False
    transmission_mode: bool = False


ETL_ONLY = SlivarMethods()


def slivar_inheritance_methods(vcf: VCF) -> SlivarMethods:
    """Slivar for the parental origin when the header declares a ``po_*`` tag, and for the transmission mode when it
    declares a mode tag with the Radiant names. A VCF from a revision before BIOINFO-239 declares ``po_*`` but the old
    mode names (``denovo``, ``recessive``…): its parental origin comes from slivar, its mode from the ETL."""
    info_ids = {h.info()["ID"] for h in vcf.header_iter() if h.type == "INFO"}
    return SlivarMethods(
        parental_origin=any(tag in info_ids for tag in PO_TAGS),
        transmission_mode=any(tag in info_ids for tag in MODE_ORDER),
    )


def _listed(record: Variant, tag: str, aliquot: str) -> bool:
    value = record.INFO.get(tag)
    if value is None:
        return False
    return aliquot in str(value).split(",")


def slivar_parental_origin(record: Variant, aliquot: str) -> str | None:
    return next((origin for tag, origin in PO_TAGS.items() if _listed(record, tag, aliquot)), None)


def slivar_transmission_mode(record: Variant, aliquot: str) -> tuple[str | None, list[str], bool]:
    """``(transmission_mode, other_transmission_modes, candidate)`` of one sample."""
    modes = [mode for mode in MODE_ORDER if _listed(record, mode, aliquot)]
    candidate = _listed(record, CANDIDATE_TAG, aliquot)
    if modes:
        return modes[0], modes[1:], candidate
    genotype = next((tag for tag in GENOTYPE_TAGS if _listed(record, tag, aliquot)), None)
    return genotype, [], candidate
