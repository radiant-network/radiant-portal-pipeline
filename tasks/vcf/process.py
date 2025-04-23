import logging

from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from tasks.iceberg.table_accumulator import TableAccumulator
from tasks.vcf.common import process_common
from tasks.vcf.consequence import parse_csq_header, process_consequence
from tasks.vcf.experiment import Case
from tasks.vcf.occurrence import process_occurrence
from tasks.vcf.pedigree import Pedigree
from tasks.vcf.variant import process_variant

logger = logging.getLogger(__name__)

CHROMOSOMES = [f"chr{i}" for i in range(1, 23)] + [
    "chrX",
    "chrY",
    "chrM",
]
GROUPED_CHROMOSOMES = [
    ["chrM", "chr18", "chr1"],
    ["chr21", "chr10", "chr2"],
    ["chr22", "chr11", "chr3"],
    ["chr20", "chr9", "chr4"],
    ["chr19", "chr8", "chr5"],
    ["chrY", "chr7", "chr6"],
    ["chr17", "chr13", "chr12"],
    ["chr14", "chr15", "chr16"],
]


def process_chromosomes(
    chromosomes: list[str],
    case: Case,
    fs,
    catalog_name="default",
    namespace="radiant",
    vcf_threads=None,
    catalog_properties=None,
):
    if catalog_properties:
        catalog = load_catalog(catalog_name, **catalog_properties)
    else:
        catalog = load_catalog(catalog_name)

    vcf = VCF(
        case.vcf_file,
        strict_gt=True,
        threads=vcf_threads,
        samples=[exp.sample_id for exp in case.experiments],
    )
    if not vcf.samples:
        raise ValueError(f"Case {case.case_id} has no matching samples in the VCF file {case.vcf_file}")

    csq_header = parse_csq_header(vcf)
    pedigree = Pedigree(case, vcf.samples)
    for chromosome in chromosomes:
        logger.info(f"Starting processing chromosome: {chromosome}")
        parsed_chromosome = chromosome.replace("chr", "")

        occurrence_table = catalog.load_table(f"{namespace}.germline_snv_occurrences")
        occurrence_partition_filters = [
            {
                "case_id": case.case_id,
                "seq_id": exp.seq_id,
                "chromosome": parsed_chromosome,
            }
            for exp in case.experiments
        ]
        occurrence_buffers = {
            occurrence_partition_filter["seq_id"]: TableAccumulator(
                occurrence_table, fs=fs, partition_filter=occurrence_partition_filter
            )
            for occurrence_partition_filter in occurrence_partition_filters
        }

        variant_csq_partition_filter = {
            "case_id": case.case_id,
            "chromosome": parsed_chromosome,
        }
        variant_table = catalog.load_table(f"{namespace}.germline_snv_variants")
        variant_buffer = TableAccumulator(variant_table, fs=fs, partition_filter=variant_csq_partition_filter)

        consequence_table = catalog.load_table(f"{namespace}.germline_snv_consequences")
        consequence_buffer = TableAccumulator(consequence_table, fs=fs, partition_filter=variant_csq_partition_filter)
        for record in vcf(chromosome):
            if len(record.ALT) <= 1:
                common = process_common(record, case_id=case.case_id)
                picked_consequence, consequences = process_consequence(record, csq_header, common)
                consequence_buffer.extend(consequences)
                occurrences = process_occurrence(record, pedigree, common=common)
                for seq_id, occ in occurrences.items():
                    occurrence_buffers[seq_id].append(occ)
                variant = process_variant(record, picked_consequence, common)
                variant_buffer.append(variant)
            else:
                logger.debug(
                    f"Skipped record {record.CHROM} - {record.POS} - {record.ALT} in file {case.vcf_file} : this is a multi allelic variant, mult-allelic are not supported. Please split vcf file."
                )

        for occurence_buffer in occurrence_buffers.values():
            occurence_buffer.write_files()
        variant_buffer.write_files()
        consequence_buffer.write_files()
        logger.info(f"✅ IMPORTED Experiment: {case.case_id}, file {case.vcf_file}, chromosome {chromosome}")
    vcf.close()
