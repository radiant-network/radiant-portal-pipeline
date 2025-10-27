import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.utils.dates import days_ago

from radiant.dags import IS_AWS, NAMESPACE, ECSEnv, get_namespace

LOGGER = logging.getLogger(__name__)


default_args = {
    "owner": "radiant",
}

PATH_TO_PYTHON_BINARY = os.getenv("RADIANT_PYTHON_PATH", "/home/airflow/.venv/radiant/bin/python")

dag_params = {
    "cases": Param(
        default=[],
        description="An array of objects representing Cases to be processed",
        type="array",
    )
}


with DAG(
    dag_id=f"{NAMESPACE}-import-germline-snv-vcf",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["radiant", "iceberg"],
    dag_display_name="Radiant - Import Germline SNV VCF",
    catchup=False,
    params=dag_params,
    max_active_tasks=128,
) as dag:
    namespace = get_namespace()

    @task(task_display_name="[PyOp] Get Cases")
    def get_cases(params):
        from radiant.tasks.vcf.experiment import Case

        if IS_AWS:
            return [
                {"case": Case.model_validate(c).model_dump()} for c in params.get("cases", []) if c.get("vcf_filepath")
            ]

        return [Case.model_validate(c).model_dump() for c in params.get("cases", []) if c.get("vcf_filepath")]

    @task(task_display_name="[PyOp] Merge Commits")
    def merge_commits(partition_lists: list[dict[str, list[dict]]] | list[str], ecs_env: ECSEnv | None = None):
        import json
        import sys
        import tempfile
        from collections import defaultdict

        import boto3

        if not partition_lists:
            return {} if not IS_AWS else json.dumps({})

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)

        if isinstance(partition_lists[0], str):
            logger.warning("Received partition lists as string")
            _parsed_partitions = []
            for part in partition_lists:
                logger.info(part)
                _parsed_partitions.append(json.loads(part))
            partition_lists = _parsed_partitions

        merged = defaultdict(list)
        for d in partition_lists:
            for table, partitions in d.items():
                merged[table].extend(partitions)

        if IS_AWS:
            # ECS limits the length of the command override, so we need to upload the merged partitions to S3
            # and pass the S3 path of the file in which the data is store to the ECS operator instead of the data.
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
                json.dump(dict(merged), tmpfile)
                tmpfile_path = tmpfile.name

            s3_client = boto3.client("s3")
            bucket_name = ecs_env.ECS_S3_WORKSPACE
            s3_key = f"tmp/commit_partitions_{os.path.basename(tmpfile_path)}"

            s3_client.upload_file(tmpfile_path, bucket_name, s3_key)
            s3_path = f"s3://{bucket_name}/{s3_key}"

            os.remove(tmpfile_path)
            return [{"table_partitions": s3_path}]

        return dict(merged)

    if IS_AWS:
        try:
            from radiant.dags.operators import ecs
        except ImportError as ie:
            LOGGER.error("ECS provider not found. Please install the required provider.")
            raise ie

        ecs_env = ECSEnv()

        ecs_create_parquet_files = ecs.ImportGermlineSNVVCF.get_create_parquet_files(
            radiant_namespace=namespace,
            ecs_env=ecs_env,
        )

        ecs_commit_partitions = ecs.ImportGermlineSNVVCF.get_commit_partitions(
            radiant_namespace=namespace,
            ecs_env=ecs_env,
        )

    else:
        try:
            from radiant.dags.operators import k8s
        except ImportError as ie:
            LOGGER.error("Kubernetes provider not found. Please install the required provider.")
            raise ie

        k8s_create_parquet_files = k8s.ImportGermlineSNVVCF.get_create_parquet_files(radiant_namespace=namespace)
        k8s_commit_partitions = k8s.ImportGermlineSNVVCF.get_commit_partitions(radiant_namespace=namespace)

    all_cases = get_cases()

    if IS_AWS:
        partition_commit = ecs_create_parquet_files.expand(params=all_cases)
        merged_commits = merge_commits(partition_commit.output, ecs_env)
        ecs_commit_partitions.expand(params=merged_commits)
    else:
        partition_commit = k8s_create_parquet_files.expand(case=all_cases)
        merged_commits = merge_commits(partition_commit)
        k8s_commit_partitions(table_partitions=merged_commits)
