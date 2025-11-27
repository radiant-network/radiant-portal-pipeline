import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.utils.dates import days_ago

from radiant.dags import IS_AWS, NAMESPACE, ECSEnv, get_namespace
from radiant.dags.operators.utils import s3_store_content

if IS_AWS:
    from radiant.dags.operators import ecs as operators
else:
    from radiant.dags.operators import k8s as operators

LOGGER = logging.getLogger(__name__)


default_args = {
    "owner": "radiant",
}

PATH_TO_PYTHON_BINARY = os.getenv("RADIANT_PYTHON_PATH", "/home/airflow/.venv/radiant/bin/python")

dag_params = {
    "tasks": Param(
        default=[],
        description="An array of objects representing Tasks to be processed",
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

    @task(task_display_name="[PyOp] Get Tasks")
    def get_tasks(params):
        from radiant.tasks.vcf.experiment import RADIANT_GERMLINE_ANNOTATION_TASK, build_task_from_dict

        _tasks = [
            build_task_from_dict(t).model_dump()
            for t in params.get("tasks", [])
            if t.get("task_type") == RADIANT_GERMLINE_ANNOTATION_TASK
        ]

        if IS_AWS:
            # Because ECS task operator doesn't support the TaskAPI, we need to return the tasks as a list of dicts
            # representing task params to map instead of a list of Task dictionaries.
            return [{"radiant_task": t} for t in _tasks]

        return _tasks

    @task(task_display_name="[PyOp] Merge Commits")
    def merge_commits(partition_lists: list[dict[str, list[dict]]] | list[str], ecs_env: ECSEnv | None = None):
        import json
        import sys
        from collections import defaultdict

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
            s3_path = s3_store_content(content=dict(merged), ecs_env=ecs_env, prefix="commit_partitions")
            return [{"table_partitions": s3_path}]

        return dict(merged)

    if IS_AWS:
        ecs_env = ECSEnv()

        ecs_create_parquet_files = operators.ImportGermlineSNVVCF.get_create_parquet_files(
            radiant_namespace=namespace,
            ecs_env=ecs_env,
        )

        ecs_commit_partitions = operators.ImportGermlineSNVVCF.get_commit_partitions(
            radiant_namespace=namespace,
            ecs_env=ecs_env,
        )

    else:
        k8s_create_parquet_files = operators.ImportGermlineSNVVCF.get_create_parquet_files(radiant_namespace=namespace)
        k8s_commit_partitions = operators.ImportGermlineSNVVCF.get_commit_partitions(radiant_namespace=namespace)

    all_tasks = get_tasks()

    if IS_AWS:
        partition_commit = ecs_create_parquet_files.expand(params=all_tasks)
        merged_commits = merge_commits(partition_commit.output, ecs_env)
        ecs_commit_partitions.expand(params=merged_commits)
    else:
        partition_commit = k8s_create_parquet_files.expand(radiant_task=all_tasks)
        merged_commits = merge_commits(partition_commit)
        k8s_commit_partitions(table_partitions=merged_commits)
