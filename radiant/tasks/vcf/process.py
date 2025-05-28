import logging

from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.iceberg.table_accumulator import TableAccumulator
from radiant.tasks.tracing.trace import get_tracer
from radiant.tasks.utils import capture_libc_stderr_and_check_errors
from radiant.tasks.vcf.common import process_common
from radiant.tasks.vcf.consequence import parse_csq_header, process_consequence
from radiant.tasks.vcf.experiment import Case
from radiant.tasks.vcf.occurrence import process_occurrence
from radiant.tasks.vcf.pedigree import Pedigree
from radiant.tasks.vcf.variant import process_variant

logger = logging.getLogger("airflow.task")
tracer = get_tracer(__name__)


# Required decoration because cyvcf2 doesn't fail when it encounters an error, it just prints to stderr.
# Airflow will treat the task as successful if the error is not captured properly.
@capture_libc_stderr_and_check_errors(error_patterns=["[E::"])
def process_chromosomes(
    chromosomes: list[str],
    case: Case,
    fs,
    catalog_name="default",
    namespace="radiant",
    vcf_threads=None,
    catalog_properties=None,
):
    with tracer.start_as_current_span(f"process_chromosomes_{str(chromosomes)}"):
        catalog = (
            load_catalog(catalog_name, **catalog_properties) if catalog_properties else load_catalog(catalog_name)
        )

        vcf = VCF(
            case.vcf_filepath,
            strict_gt=True,
            threads=vcf_threads,
            samples=[exp.sample_id for exp in case.experiments],
        )
        if not vcf.samples:
            raise ValueError(f"Case {case.case_id} has no matching samples in the VCF file {case.vcf_filepath}")

        csq_header = parse_csq_header(vcf)
        pedigree = Pedigree(case, vcf.samples)
        for chromosome in chromosomes:
            with tracer.start_as_current_span(f"chromosome_{chromosome}"):
                logger.info(f"Starting processing chromosome: {chromosome}")
                parsed_chromosome = chromosome.replace("chr", "")

                occurrence_table = catalog.load_table(f"{namespace}.germline_snv_occurrence")
                occurrence_partition_filters = [
                    {
                        "part": case.part,
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
                variant_table = catalog.load_table(f"{namespace}.germline_snv_variant")
                variant_buffer = TableAccumulator(variant_table, fs=fs, partition_filter=variant_csq_partition_filter)

                consequence_table = catalog.load_table(f"{namespace}.germline_snv_consequence")
                consequence_buffer = TableAccumulator(
                    consequence_table, fs=fs, partition_filter=variant_csq_partition_filter
                )
                for record in vcf(chromosome):
                    if len(record.ALT) <= 1:
                        common = process_common(record, case_id=case.case_id, part=case.part)
                        picked_consequence, consequences = process_consequence(record, csq_header, common)
                        consequence_buffer.extend(consequences)
                        occurrences = process_occurrence(record, pedigree, common=common)
                        for seq_id, occ in occurrences.items():
                            occurrence_buffers[seq_id].append(occ)
                        variant = process_variant(record, picked_consequence, common)
                        variant_buffer.append(variant)

                    else:
                        logger.debug(
                            f"Skipped record {record.CHROM} - {record.POS} - {record.ALT} in file {case.vcf_filepath}:"
                            f" this is a multi allelic variant, mult-allelic are not supported. Please split vcf file."
                        )

                for occurrence_buffer in occurrence_buffers.values():
                    occurrence_buffer.write_files()
                variant_buffer.write_files()
                consequence_buffer.write_files()
                logger.info(
                    f"✅ IMPORTED Experiment: {case.case_id}, file {case.vcf_filepath}, chromosome {chromosome}"
                )

        vcf.close()
