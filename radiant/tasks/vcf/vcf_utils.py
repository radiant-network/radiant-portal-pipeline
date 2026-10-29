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
