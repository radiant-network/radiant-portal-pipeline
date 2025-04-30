from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from radiant.dags import NAMESPACE

default_args = {
    "owner": "radiant",
}

PATH_TO_PYTHON_BINARY = "/home/airflow/.venv/radiant/bin/python"

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

with DAG(
    dag_id=f"{NAMESPACE}-radiant-import-vcf",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["radiant", "iceberg"],
    catchup=False,
) as dag:

    @task
    def get_cases():
        from radiant.tasks.vcf.experiment import Case, Experiment

        cases = [
            Case(
                case_id=i,
                vcf_file="s3://cqdg-qa-file-import/jmichaud/study1/dataset_data2/annotated_vcf/variants.FM0000398.vep.vcf.gz",
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

    @task.external_python(pool="import_vcf", task_id="import_vcf", python=PATH_TO_PYTHON_BINARY)
    def import_vcf(case: dict, chromosomes: list[str]):
        import logging
        import sys

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)

        import os

        import fsspec

        from radiant.tasks.vcf.experiment import Case
        from radiant.tasks.vcf.process import process_chromosomes

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
