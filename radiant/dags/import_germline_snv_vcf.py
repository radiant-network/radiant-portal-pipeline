import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param, Variable
from airflow.utils.dates import days_ago

from radiant.dags import NAMESPACE

LOGGER = logging.getLogger(__name__)


def parse_list(env_val):
    return [v.strip() for v in env_val.split(",") if v.strip()]


IS_AWS = os.environ.get("IS_AWS", "false").lower() == "true"
if IS_AWS:
    LOGGER.info("Running in AWS environment")
    ECS_CLUSTER = Variable.get("AWS_ECS_CLUSTER")
    ECS_SUBNETS = parse_list(Variable.get("AWS_ECS_SUBNETS"))
    ECS_SECURITY_GROUPS = parse_list(Variable.get("AWS_ECS_SECURITY_GROUPS"))


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

    @task
    def get_cases_ecs(params):
        from radiant.tasks.vcf.experiment import Case

        return [
            {"case": Case.model_validate(c).model_dump()} for c in params.get("cases", []) if c.get("vcf_filepath")
        ]

    @task
    def get_cases_k8s(params):
        from radiant.tasks.vcf.experiment import Case

        return [Case.model_validate(c).model_dump() for c in params.get("cases", []) if c.get("vcf_filepath")]

    if IS_AWS:
        try:
            from airflow.providers.amazon.aws.operators import ecs
        except ImportError as ie:
            LOGGER.error("ECS operator not found. Please install the required provider.")
            raise ie

        create_parquet_files = ecs.EcsRunTaskOperator.partial(
            pool="import_vcf",
            task_id="create_parquet_files",
            task_definition="airflow_ecs_operator_task:13",
            cluster=ECS_CLUSTER,
            launch_type="FARGATE",
            awslogs_group="apps-qa/radiant-etl",
            awslogs_region="us-east-1",
            # There's a bug in the 9.2.0 provider that forces to add the container name as well
            awslogs_stream_prefix="ecs/radiant-operator-qa-etl-container",
            awslogs_fetch_interval=timedelta(seconds=5),
            overrides={
                "containerOverrides": [
                    {
                        "name": "radiant-operator-qa-etl-container",
                        "command": ["python /opt/radiant/import_vcf_for_case.py --case '{{ params.case | tojson }}'"],
                        "environment": [
                            {"name": "PYTHONPATH", "value": "/opt/radiant"},
                            {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                            {"name": "RADIANT_ICEBERG_NAMESPACE", "value": "radiant_qa"},
                            {"name": "PYICEBERG_CATALOG__DEFAULT__TYPE", "value": "glue"},
                            {"name": "STARROCKS_BROKER_USE_INSTANCE_PROFILE", "value": "true"},
                        ],
                    }
                ]
            },
            # TODO : Are we forced to specify this here or can we use the ECS task definition instead ?
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": ECS_SUBNETS,
                    "assignPublicIp": "DISABLED",
                    "securityGroups": ECS_SECURITY_GROUPS,
                }
            },
            aws_conn_id="aws_default",
            region_name="us-east-1",
        )
    else:
        try:
            import airflow.providers.cncf.kubernetes.operators.pod  # noqa: F401
        except ImportError as ie:
            LOGGER.error("Kubernetes provider not found. Please install the required provider.")
            raise ie

        @task.kubernetes(
            kubernetes_conn_id="kubernetes_conn",
            pool="import_vcf",
            task_id="create_parquet_files",
            map_index_template="Case: {{ task.op_kwargs['case']['case_id'] }}",
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
        def create_parquet_files(case: dict, namespace: str):
            from radiant.tasks.vcf.snv.germline.process import create_parquet_files

            return create_parquet_files(case=case, namespace=namespace)

    @task
    def merge_commits(partition_lists: list[dict[str, list[dict]]]) -> dict[str, list[dict]]:
        from radiant.tasks.vcf.snv.germline.process import merge_commits

        return merge_commits(partition_lists)

    @task.external_python(task_id="commit_partitions", python=PATH_TO_PYTHON_BINARY)
    def commit_partitions(table_partitions: dict[str, list[dict]]):
        from radiant.tasks.vcf.snv.germline.process import commit_partitions

        return commit_partitions(table_partitions)

    if IS_AWS:
        all_cases = get_cases_ecs()
        partitions_commit = create_parquet_files.expand(params=all_cases)
        merged_commit = merge_commits(partitions_commit.output)
    else:
        all_cases = get_cases_k8s()
        partitions_commit = create_parquet_files.expand(case=all_cases)
        merged_commit = merge_commits(partitions_commit)

    commit_partitions(merged_commit)
