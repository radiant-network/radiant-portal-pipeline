import logging

from airflow import DAG

LOGGER = logging.getLogger(__name__)


def create_portable_task(task_id):
    import os
    import urllib3
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    os.environ["PYTHONHTTPSVERIFY"] = "0"
    os.environ["REQUESTS_CA_BUNDLE"] = ""

    return KubernetesPodOperator(
        kubernetes_conn_id="kubernetes_conn",
        task_id=task_id,
        name="run-on-k8s",
        namespace="radiant",
        image="radiant-airflow",
        image_pull_policy="Never",
        cmds=["bash", "-c", "source /home/airflow/.venv/radiant/bin/activate && python -m pip freeze"],
        get_logs=True,
        is_delete_operator_pod=True,
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
