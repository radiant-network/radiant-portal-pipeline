import logging

from airflow import DAG

LOGGER = logging.getLogger(__name__)


def create_portable_task(task_id):
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    return KubernetesPodOperator(
        kubernetes_conn_id="kubernetes_conn",
        task_id=task_id,
        name="run-on-k8s",
        namespace="radiant",
        image="radiant-airflow:latest",
        image_pull_policy="IfNotPresent",
        cmds=["bash", "-c", "cd", "/home/airflow", "&&", "./run.sh"],
        get_logs=True,
        is_delete_operator_pod=True,
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
        }
    )
    # else:
    #     from airflow.providers.amazon.aws.operators.ecs import ECSOperator
    #     return ECSOperator(
    #         task_id=task_id,
    #         cluster="my-cluster",
    #         task_definition="my-task-def",
    #         launch_type="FARGATE",
    #         overrides={
    #             "containerOverrides": [{
    #                 "name": "my-container",
    #                 "command": ["python", "my_script.py"]
    #             }]
    #         },
    #         network_configuration={
    #             "awsvpcConfiguration": {
    #                 "subnets": ["subnet-abc123"],
    #                 "assignPublicIp": "ENABLED",
    #                 "securityGroups": ["sg-abc123"]
    #             }
    #         },
    #     )


with DAG(
    dag_id="POC-ecs-k8s-operators",
    schedule_interval=None,
    catchup=False,
    tags=["radiant", "poc", "manual"],
    dag_display_name="[POC] ECS/K8s Operators",
) as dag:
    task = create_portable_task("run_compute")
