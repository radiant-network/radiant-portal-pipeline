from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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

dag_params = {
    "cases": Param(
        default=[],
        description="An array of objects representing Cases to be processed",
        type="array",
    )
}

with DAG(
    dag_id=f"{NAMESPACE}-import-vcf",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["radiant", "iceberg"],
    dag_display_name="Radiant - Import VCF",
    catchup=False,
    params=dag_params,
    max_active_tasks=128,
) as dag:

    @task
    def get_cases(params):
        from radiant.tasks.vcf.experiment import Case

        return [Case.model_validate(c).model_dump() for c in params.get("cases", [])]

    @task
    def generate_presigned_url(case: dict, expiration=3600):
        hook = S3Hook(aws_conn_id='minio')
        client = hook.get_conn()

        if case['vcf_filepath'].startswith("s3://"):
            bucket_name, object_key = case['vcf_filepath'][5:].split("/", 1)
            vcf_url = client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=expiration
            )
            tbi_url = client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': f"{object_key}.tbi"},
                ExpiresIn=expiration
            )
            case['vcf_filepath'] = vcf_url
            case['tbi_filepath'] = tbi_url

        return case

    @task.external_python(pool="import_vcf", task_id="import_vcf", python=PATH_TO_PYTHON_BINARY)
    def import_vcf(case: dict, chromosomes: list[str]):
        import logging
        import os
        import sys

        import fsspec

        from radiant.tasks.vcf.experiment import Case
        from radiant.tasks.vcf.process import process_chromosomes

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)

        fs = fsspec.filesystem(
            "s3",
            client_kwargs={"endpoint_url": os.environ.get("PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT")},
        )
        case = Case.model_validate(case)
        logger.info(f"🔁 STARTING IMPORT for Case: {case.case_id}, chromosome {','.join(chromosomes)}")
        logger.info("=" * 80)

        namespace = os.getenv("RADIANT_ICEBERG_DATABASE", "radiant")
        process_chromosomes(chromosomes, case, fs, namespace=namespace, vcf_threads=None)
        logger.info(
            f"✅ IMPORTED Experiment: {case.case_id}, file {case.vcf_filepath}, chromosome {','.join(chromosomes)}"
        )

    all_cases = get_cases()

    generate_url = generate_presigned_url.expand(all_cases)
    import_vcf.expand(case=generate_url, chromosomes=GROUPED_CHROMOSOMES)
