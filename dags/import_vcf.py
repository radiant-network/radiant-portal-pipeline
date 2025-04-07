import logging
import os

import fsspec
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from tasks.vcf.experiment import Case, Experiment
from tasks.vcf.process import process_chromosomes, GROUPED_CHROMOSOMES

logger = logging.getLogger(__name__)
default_args = {
    "owner": "radiant",
}

with DAG(
    dag_id="import_vcf",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    @task
    def get_cases():
        cases = [
            Case(
                case_id=i,
                vcf_file="s3+http://vcf/variants.FM0000398.vep.vcf.gz",
                experiments=[
                    Experiment(
                        seq_id=i,
                        patient_id=f"pa00{i}",
                        sample_id="S14018",
                        family_role="proband",
                        is_affected=True,
                        sex="F",
                    ),
                ],
            )
            for i in range(1, 2)
        ]
        return [case.model_dump() for case in cases]

    @task(pool="import_vcf")
    def import_vcf(case: dict, chromosomes: list[str]):
        fs = fsspec.filesystem(
            "s3",
            client_kwargs={"endpoint_url": os.environ.get("PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT")},
        )
        case = Case.model_validate(case)
        logger.info(f"🔁 STARTING IMPORT for Case: {case.case_id}, chromosome {','.join(chromosomes)}")
        logger.info("=" * 80)
        process_chromosomes(chromosomes, case, fs, vcf_threads=4)
        logger.info(
            f"✅ IMPORTED Experiment: {case.case_id}, file {case.vcf_file}, chromosome {','.join(chromosomes)}"
        )

    all_cases = get_cases()

    import_vcf.expand(case=all_cases, chromosomes=GROUPED_CHROMOSOMES)
