import logging
import os

import fsspec
from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog
import traceback
from vcf.common import process_common
from vcf.consequence import parse_csq_header, process_consequence
from vcf.experiment import Case, Experiment
from vcf.occurrence import process_occurrence
from vcf.pedigree import Pedigree
from iceberg.table_accumulator import TableAccumulator
from vcf.variant import process_variant

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
    chromosomes: list[str], case: Case, fs, catalog_name="default", namespace="radiant"
):
    catalog = load_catalog(catalog_name)

    vcf = VCF(
        case.vcf_file,
        strict_gt=True,
        threads=4,
        samples=[exp.sample_id for exp in case.experiments],
    )
    if not vcf.samples:
        raise ValueError(
            f"Case {case.case_id} has no matching samples in the VCF file {case.vcf_file}"
        )

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
        variant_buffer = TableAccumulator(
            variant_table, fs=fs, partition_filter=variant_csq_partition_filter
        )

        consequence_table = catalog.load_table(f"{namespace}.germline_snv_consequences")
        consequence_buffer = TableAccumulator(
            consequence_table, fs=fs, partition_filter=variant_csq_partition_filter
        )
        for record in vcf(chromosome):
            if len(record.ALT) <= 1:
                common = process_common(record, case_id=case.case_id)
                picked_consequence, consequences = process_consequence(
                    record, csq_header, common
                )
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
        logger.info(
            f"✅ IMPORTED Experiment: {case.case_id}, file {case.vcf_file}, chromosome {chromosome}"
        )
    vcf.close()


def process_chromosome_logging(
    chromosomes: list[str], case: Case, fs, catalog_name="default", namespace="radiant"
):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(processName)s - %(levelname)s - %(name)s - %(message)s",
    )
    return process_chromosomes(chromosomes, case, fs, catalog_name, namespace)


if __name__ == "__main__":

    example_case2 = Case(
        case_id=1002,
        vcf_file="s3+http://radiant@play/vcf/variants.HSJ0787.vep.vcf.gz",
        experiments=[
            Experiment(
                seq_id=1,
                patient_id="pa001",
                sample_id="S24379",
                family_role="proband",
                is_affected=True,
                sex="F",
            ),
            Experiment(
                seq_id=2,
                patient_id="pa002",
                sample_id="S24380",
                family_role="father",
                is_affected=True,
                sex="M",
            ),
            Experiment(
                seq_id=3,
                patient_id="pa003",
                sample_id="S24381",
                family_role="mother",
                is_affected=False,
                sex="F",
            ),
        ],
    )

    example_case1 = Case(
        case_id=1003,
        vcf_file="s3+http://radiant@play/vcf/variants.11100002.vep.vcf.gz",
        experiments=[
            Experiment(
                seq_id=1,
                patient_id="pa001",
                sample_id="11100002",
                family_role="proband",
                is_affected=False,
                sex="F",
            ),
        ],
    )
    currentFs = fsspec.filesystem(
        "s3",
        client_kwargs={
            "endpoint_url": os.environ.get("PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT")
        },
    )
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(max_workers=1) as executor:
        futures = [
            executor.submit(
                process_chromosome_logging, chromosomes, example_case1, currentFs
            )
            for chromosomes in [["chr1"]]
        ]
        for future in futures:
            try:
                result = future.result()
                print(f"Result: {result}")
            except Exception as e:
                print(f"Error in processing: {e}")
                traceback.print_exc()
