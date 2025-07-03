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

        return [Case.model_validate(c).model_dump() for c in params.get("cases", [])]

    @task.external_python(
        pool="import_vcf",
        task_id="create_parquet_files",
        python=PATH_TO_PYTHON_BINARY,
        map_index_template=("Case: {{ task.op_kwargs['case']['case_id'] }}"),
    )
    def create_parquet_files(case: dict):
        import json
        import logging
        import os
        import sys
        from urllib import parse

        import boto3

        from radiant.tasks.vcf.experiment import Case
        from radiant.tasks.vcf.process import process_case

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)

        def generate_presigned_url(s3_path, expiration=3600):
            """
            Generate a presigned URL to share an S3 object

            :param bucket_name: string
            :param object_key: string
            :param expiration: Time in seconds for the presigned URL to remain valid (default: 1 hour)
            :return: Presigned URL as string. If error, returns None.
            """
            s3_client = boto3.client("s3")

            def extract_bucket_key(s3_path):
                parsed = parse.urlparse(s3_path)
                bucket = parsed.netloc
                key = parsed.path.lstrip("/")
                return bucket, key

            bucket_name, object_key = extract_bucket_key(s3_path)
            try:
                response = s3_client.generate_presigned_url(
                    "get_object", Params={"Bucket": bucket_name, "Key": object_key}, ExpiresIn=expiration
                )
            except Exception as e:
                print(f"Error generating presigned URL: {e}")
                return None

            return response

        logger.info("Generating presigned URLs for VCF and index files")
        url = generate_presigned_url(case["vcf_filepath"], expiration=7200)
        index_url = generate_presigned_url(case["vcf_filepath"] + ".tbi", expiration=7200)
        case["vcf_filepath"] = url
        case["index_vcf_filepath"] = index_url

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
