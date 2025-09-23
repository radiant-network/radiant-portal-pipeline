from cyvcf2 import Variant

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
