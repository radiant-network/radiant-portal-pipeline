import math
from collections.abc import Callable
from typing import Any

from cyvcf2 import VCF, Variant

ZYGOSITY_WT = 0
ZYGOSITY_HET = 1
ZYGOSITY_HOM = 3
ZYGOSITY_UNK = 2
ZYGOSITY = {
    ZYGOSITY_WT: "WT",
    ZYGOSITY_HET: "HET",
    ZYGOSITY_HOM: "HOM",
    ZYGOSITY_UNK: "UNK",
}


def calls_without_phased(record: Variant, sample_idx: int) -> list[int]:
    """Extracts genotype calls for a given sample index, excluding phased information.

    Args:
        record (Variant): A `cyvcf2.Variant` object representing the genetic variant.
        sample_idx (int): The index of the sample in the VCF record.

    Returns:
        list[int]: A list of genotype calls without phased information.
    """
    return record.genotypes[sample_idx][:-1]  # Exclude phased information


# cyvcf2 returns htslib's raw Integer FORMAT values (htslib/vcf.h): `bcf_int32_missing` (INT32 min) for `.`, and
# `bcf_int32_vector_end` (INT32 min + 1) for the padding after a vector shorter than the record's longest. A Float
# value, missing or padding, comes back as NaN.
INT_MISSING = -(2**31)
INT_VECTOR_END = -(2**31) + 1


def format_array(record: Variant, field: str):
    """All the samples' values of one FORMAT field, or None when the record has none.

    cyvcf2 gives None for a field the header declares but the record leaves out, and raises `KeyError` for a field
    the header does not declare (`Variant.format` reads the field's type from the header first).
    """
    try:
        return record.format(field)
    except KeyError:
        return None


def _value_or_none(value, cast: Callable[[Any], Any]) -> Any:
    # The cast is explicit so the value matches the Iceberg column, whatever type the header declares.
    numeric = float(value)
    if math.isnan(numeric) or numeric in (INT_MISSING, INT_VECTOR_END):
        return None
    return cast(value)


def format_value(record: Variant, field: str, sample_idx: int, cast: Callable[[Any], Any]) -> Any:
    """The first value of one numeric FORMAT field for one sample. None when the field is not declared, not in the
    record, or `.` for the sample."""
    values = format_array(record, field)
    return _value_or_none(values[sample_idx][0], cast) if values is not None else None


def format_values(record: Variant, field: str, sample_idx: int, cast: Callable[[Any], Any]) -> list | None:
    """All the values of one numeric FORMAT field for one sample. The vector stops at the padding; a `.` inside it
    is a None element. None when the field is absent (as `format_value`) or every value is `.`."""
    values = format_array(record, field)
    if values is None:
        return None
    items = []
    for value in values[sample_idx]:
        if float(value) == INT_VECTOR_END:
            break
        items.append(_value_or_none(value, cast))
    return None if all(v is None for v in items) else items


# Spellings some callers or versions use for an INFO field, besides the name the ETL reads it under.
INFO_ALIASES = {"InbreedingCoeff": ("InbreedCoeff",)}


class InfoKeys:
    """Reads INFO fields under the ID the VCF header declares them, whatever the spelling the ETL uses.

    The spelling is the callers' and moves across tools and versions (`VQSLOD`, `culprit`,
    `InbreedingCoeff`), while cyvcf2 looks INFO up case-sensitively: a wrong spelling reads None on every
    record, silently. A name matches a header ID ignoring case, then through `INFO_ALIASES`; each name is
    resolved once and cached. Built without a VCF, it reads every name as is.
    """

    def __init__(self, vcf: VCF | None = None):
        self._header_ids = (
            None
            if vcf is None
            else {h.info()["ID"].lower(): h.info()["ID"] for h in vcf.header_iter() if h.type == "INFO"}
        )
        self._keys: dict[str, str | None] = {}

    def _key(self, name: str) -> str | None:
        if self._header_ids is None:
            return name
        if name not in self._keys:
            candidates = (name, *INFO_ALIASES.get(name, ()))
            self._keys[name] = next(
                (self._header_ids[c.lower()] for c in candidates if c.lower() in self._header_ids), None
            )
        return self._keys[name]

    def value(self, info, name: str):
        key = self._key(name)
        return info.get(key, None) if key is not None else None
