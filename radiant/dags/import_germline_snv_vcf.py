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
    def merge_commits(partition_lists: list[dict[str, list[dict]]] | list[str]) -> dict[str, list[dict]]:
        import json
        import sys
        from collections import defaultdict

        if not partition_lists:
            return {}

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)

        if isinstance(partition_lists[0], str):
            logger.warning("Received partition lists as string")
            _parsed_partitions = []
            for part in partition_lists:
                _parsed_partitions.append(json.loads(part))
            partition_lists = _parsed_partitions

        merged = defaultdict(list)
        for d in partition_lists:
            for table, partitions in d.items():
                merged[table].extend(partitions)
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
    partition_commits = (
        ecs_create_parquet_files.expand(params=all_cases)
        if IS_AWS
        else k8s_create_parquet_files.expand(case=all_cases)
    )

    if IS_AWS:
        @task
        def collect_results(**kwargs):
            # This is required because the ECS mapped operator cannot correctly
            # collect results before sending them to the following task. Therefore,
            # we need to collect them manually.
            ti = kwargs['ti']
            results = ti.xcom_pull(task_ids='ecs_create_parquet_files', include_prior_dates=False)
            return merge_commits(results)

    merged_commits = collect_results() if IS_AWS else merge_commits(partition_commits)
    commit_partitions = (
        ecs_commit_partitions.expand(params={"table_partitions": merged_commits})
        if IS_AWS
        else k8s_commit_partitions(table_partitions=merged_commits)
    )
