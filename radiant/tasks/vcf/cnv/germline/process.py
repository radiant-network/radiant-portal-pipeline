import logging

import pyarrow as pa
from cyvcf2 import VCF
from pyiceberg.catalog import load_catalog

from radiant.tasks.tracing.trace import get_tracer
from radiant.tasks.vcf.cnv.germline.occurrence import process_occurrence
from radiant.tasks.vcf.experiment import Case

logger = logging.getLogger("airflow.task")
tracer = get_tracer(__name__)


# Required decoration because cyvcf2 doesn't fail when it encounters an error, it just prints to stderr.
# Airflow will treat the task as successful if the error is not captured properly.
# @capture_libc_stderr_and_check_errors(error_patterns=["[E::"])
def process_cases(
    cases: list[Case],
    catalog_name="default",
    namespace="radiant",
    vcf_threads=None,
    catalog_properties=None,
):
    with tracer.start_as_current_span("process_cases"):
        occurrences_partition_commit = []

        catalog = (
            load_catalog(catalog_name, **catalog_properties) if catalog_properties else load_catalog(catalog_name)
        )

        occurrences_table_name = f"{namespace}.germline_cnv_occurrence"
        occurrence_table = catalog.load_table(occurrences_table_name)
        occurrence_buffer = []
        for case in cases:
            for exp in case.experiments:
                vcf = VCF(
                    exp.cnv_vcf_filepath,
                    strict_gt=True,
                    threads=vcf_threads,
                    samples=[exp.aliquot for exp in case.experiments],
                )

                if not vcf.samples:
                    raise ValueError(
                        f"Case {case.case_id} has no matching samples in the VCF file {case.vcf_filepath}"
                    )
                sample_idx = vcf.samples.index(exp.aliquot)
                part = 0
                with tracer.start_as_current_span(f"vcf_case_{case.case_id}_{exp.seq_id}"):
                    logger.info(f"Starting processing vcf for case {case.case_id} with file {case.vcf_filepath}")

                    for record in vcf:
                        occurrence = process_occurrence(record, part, exp.seq_id, exp.aliquot, sample_idx)
                        occurrence_buffer.append(occurrence)
                    vcf.close()
        df = pa.Table.from_pylist(occurrence_buffer, schema=occurrence_table.schema().as_arrow())
        occurrence_table.overwrite(df)
        logger.info(f"✅ Table {occurrences_table_name} overwritten")

        return {occurrences_table_name: occurrences_partition_commit}
