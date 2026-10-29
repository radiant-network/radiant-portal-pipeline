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


class InfoKeys(dict):
    """INFO field name as the ETL reads it -> the ID the VCF header declares it under, None when absent.

    The spelling is the callers' and moves across tools and versions (`VQSLOD`, `culprit`,
    `InbreedingCoeff`), while cyvcf2 looks INFO up case-sensitively: a wrong spelling reads None on every
    record, silently. Built once per file by `resolve_info_keys`; an empty one reads every name as is.
    """

    def value(self, info, name: str):
        key = self.get(name, name)
        return info.get(key, None) if key is not None else None

    def missing(self) -> list[str]:
        return [name for name, key in self.items() if key is None]


def resolve_info_keys(vcf: VCF, names: tuple[str, ...]) -> InfoKeys:
    """Matches each name, then its aliases, to an INFO ID of the header, ignoring case."""
    header_ids = {h.info()["ID"].lower(): h.info()["ID"] for h in vcf.header_iter() if h.type == "INFO"}
    return InfoKeys(
        {
            name: next(
                (header_ids[c.lower()] for c in (name, *INFO_ALIASES.get(name, ())) if c.lower() in header_ids), None
            )
            for name in names
        }
    )
