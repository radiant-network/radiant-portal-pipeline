import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.utils.dates import days_ago

from radiant.dags import NAMESPACE

default_args = {
    "owner": "radiant",
}

PATH_TO_PYTHON_BINARY = os.getenv("RADIANT_PYTHON_PATH", "/home/airflow/.venv/radiant/bin/python")

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

        return [Case.model_validate(c).model_dump() for c in params.get("cases", []) if c.get("vcf_filepath")]

    @task.kubernetes(
        kubernetes_conn_id="kubernetes_conn",
        task_id="create_parquet_files",
        name="import-vcf-for-case",
        namespace="radiant",
        image="radiant-vcf-operator:latest",
        image_pull_policy="Never",
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        env_vars={
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_ENDPOINT_URL": "http://host.docker.internal:9000",
            "AWS_ALLOW_HTTP": "true",
            "PYICEBERG_CATALOG__DEFAULT__URI": "http://host.docker.internal:8181",
            "PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT": "http://host.docker.internal:9000",
            "PYICEBERG_CATALOG__DEFAULT__TOKEN": "mysecret",
            "RADIANT_ICEBERG_NAMESPACE": "radiant_iceberg_namespace",
            "PYTHONPATH": "/opt/radiant",
            "LD_LIBRARY_PATH": "/usr/local/lib:$LD_LIBRARY_PATH",
        },
    )
    def create_parquet_files(case: dict):
        import json
        import logging
        import os
        import sys
        import tempfile
        from urllib import parse

        import boto3

        from radiant.tasks.vcf.experiment import Case
        from radiant.tasks.vcf.process import process_case

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)

        def download_s3_file(s3_path, dest_dir):
            s3_client = boto3.client("s3")

            def extract_bucket_key(s3_path):
                parsed = parse.urlparse(s3_path)
                bucket = parsed.netloc
                key = parsed.path.lstrip("/")
                return bucket, key

            bucket_name, object_key = extract_bucket_key(s3_path)
            local_path = os.path.join(dest_dir, os.path.basename(object_key))
            try:
                s3_client.download_file(bucket_name, object_key, local_path)
            except Exception as e:
                print(f"Error downloading S3 file: {e}")
                return None
            return local_path

        logger.info("Downloading VCF and index files to a temporary directory")
        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_local = download_s3_file(case["vcf_filepath"], tmpdir)
            index_local = download_s3_file(case["vcf_filepath"] + ".tbi", tmpdir)
            case["vcf_filepath"] = vcf_local
            case["index_vcf_filepath"] = index_local

            case = Case.model_validate(case)
            logger.info(f"🔁 STARTING IMPORT for Case: {case.case_id}")
            logger.info("=" * 80)

            namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE", "radiant")
            res = process_case(case, namespace=namespace, vcf_threads=4)
            logger.info(f"✅ Parquet files created: {case.case_id}, file {case.vcf_filepath}")

        return {k: [json.loads(pc.model_dump_json()) for pc in v] for k, v in res.items()}

    @task
    def merge_commits(partition_lists: list[dict[str, list[dict]]]) -> dict[str, list[dict]]:
        from collections import defaultdict

        merged = defaultdict(list)
        for d in partition_lists:
            for table, partitions in d.items():
                merged[table].extend(partitions)
        return dict(merged)

    @task.external_python(task_id="commit_partitions", python=PATH_TO_PYTHON_BINARY)
    def commit_partitions(table_partitions: dict[str, list[dict]]):
        import logging
        import sys

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)
        from pyiceberg.catalog import load_catalog

        from radiant.tasks.iceberg.partition_commit import PartitionCommit
        from radiant.tasks.iceberg.utils import commit_files

        catalog = load_catalog()
        for table_name, partitions in table_partitions.items():
            if not partitions:
                continue
            table = catalog.load_table(table_name)
            parts = [PartitionCommit.model_validate(pc) for pc in partitions]
            logger.info(f"🔁 Starting commit for table {table_name}")
            commit_files(table, parts)
            logger.info(f"✅ Changes commited to table {table_name}")

    all_cases = get_cases()

    partitions_commit = create_parquet_files.expand(case=all_cases)
    merged_commit = merge_commits(partitions_commit)
    commit_partitions(merged_commit)
