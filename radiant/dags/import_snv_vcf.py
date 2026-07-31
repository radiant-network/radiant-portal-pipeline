import logging
import os

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

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
    dag_id=f"{NAMESPACE}-import-snv-vcf",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["radiant", "iceberg"],
    dag_display_name="Radiant - Import SNV VCF",
    catchup=False,
    params=dag_params,
    max_active_tasks=128,
) as dag:
    # Germline and somatic extraction both fan out here, and both fan into a single commit task.
    # That is deliberate: `snv_variant` and `snv_consequence` are written by both flows, so one
    # committer per part is what keeps their Iceberg commits from racing each other.
    namespace = get_namespace()

    def _tasks_for_type(params, task_type: str):
        from radiant.tasks.vcf.experiment import build_task_from_dict

        _tasks = [
            build_task_from_dict(t).model_dump() for t in params.get("tasks", []) if t.get("task_type") == task_type
        ]

        if IS_AWS:
            # Because ECS task operator doesn't support the TaskAPI, we need to return the tasks as a list of dicts
            # representing task params to map instead of a list of Task dictionaries.
            return [{"radiant_task": t} for t in _tasks]

        return _tasks

    @task(task_id="get_germline_tasks", task_display_name="[PyOp] Get Germline Tasks")
    def get_germline_tasks(params):
        from radiant.tasks.vcf.experiment import RADIANT_GERMLINE_ANNOTATION_TASK

        return _tasks_for_type(params, RADIANT_GERMLINE_ANNOTATION_TASK)

    @task(task_id="get_somatic_tasks", task_display_name="[PyOp] Get Somatic Tasks")
    def get_somatic_tasks(params):
        from radiant.tasks.vcf.experiment import RADIANT_SOMATIC_ANNOTATION_TASK

        return _tasks_for_type(params, RADIANT_SOMATIC_ANNOTATION_TASK)

    # NONE_FAILED, not the default ALL_SUCCESS: a part with no somatic tasks (or no germline ones)
    # expands that flow's writer to zero mapped instances, which Airflow marks SKIPPED. Under
    # ALL_SUCCESS that skip would cascade here and nothing would commit for the *other* flow
    # either. A genuine extraction failure still blocks the commit, as upstream_failed.
    @task(task_display_name="[PyOp] Merge Commits", trigger_rule=TriggerRule.NONE_FAILED)
    def merge_commits(
        germline_partitions: list[dict[str, list[dict]]] | list[str],
        somatic_partitions: list[dict[str, list[dict]]] | list[str],
        ecs_env: ECSEnv | None = None,
    ):
        import json
        import sys

        # `partition_commit`, not `utils`: this PyOp runs in the Airflow interpreter, which has
        # no pyiceberg/pyarrow. See the note on `merge_partition_commits`.
        from radiant.tasks.iceberg.partition_commit import merge_partition_commits

        logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        logger = logging.getLogger(__name__)

        def _parse(partition_lists):
            # The ECS operator hands back the container's stdout, so each mapped result arrives
            # as a JSON string rather than a dict.
            parsed = []
            for part in partition_lists or []:
                if isinstance(part, str):
                    logger.info(part)
                    parsed.append(json.loads(part))
                else:
                    parsed.append(part)
            return parsed

        parsed = _parse(germline_partitions) + _parse(somatic_partitions)
        merged = merge_partition_commits(parsed)
        logger.info(f"Merged partitions from {len(parsed)} extraction task(s) across {len(merged)} table(s)")

        if IS_AWS:
            if not merged:
                logger.info("Nothing to commit — the commit task will be skipped")
                return []
            # ECS limits the length of the command override, so we need to upload the merged partitions to S3
            # and pass the S3 path of the file in which the data is store to the ECS operator instead of the data.
            s3_path = s3_store_content(content=merged, ecs_env=ecs_env, prefix="commit_partitions")
            return [{"table_partitions": s3_path}]

        return merged

    if IS_AWS:
        ecs_env = ECSEnv()

        create_germline_parquet_files = operators.ImportSNVVCF.get_create_germline_parquet_files(
            radiant_namespace=namespace,
            ecs_env=ecs_env,
        )
        create_somatic_parquet_files = operators.ImportSNVVCF.get_create_somatic_parquet_files(
            radiant_namespace=namespace,
            ecs_env=ecs_env,
        )
        commit_partitions = operators.ImportSNVVCF.get_commit_partitions(
            radiant_namespace=namespace,
            ecs_env=ecs_env,
        )

    else:
        create_germline_parquet_files = operators.ImportSNVVCF.get_create_germline_parquet_files(
            radiant_namespace=namespace
        )
        create_somatic_parquet_files = operators.ImportSNVVCF.get_create_somatic_parquet_files(
            radiant_namespace=namespace
        )
        commit_partitions = operators.ImportSNVVCF.get_commit_partitions(radiant_namespace=namespace)

    germline_tasks = get_germline_tasks()
    somatic_tasks = get_somatic_tasks()

    if IS_AWS:
        germline_commits = create_germline_parquet_files.expand(params=germline_tasks)
        somatic_commits = create_somatic_parquet_files.expand(params=somatic_tasks)

        merged_commits = merge_commits(germline_commits.output, somatic_commits.output, ecs_env)
        commit_partitions.expand(params=merged_commits)
    else:
        germline_commits = create_germline_parquet_files.expand(radiant_task=germline_tasks)
        somatic_commits = create_somatic_parquet_files.expand(radiant_task=somatic_tasks)

        merged_commits = merge_commits(germline_commits, somatic_commits)
        commit_partitions(table_partitions=merged_commits)
    merged_commits.set_upstream(namespace)  # ensure namespace is resolved before downstream tasks
