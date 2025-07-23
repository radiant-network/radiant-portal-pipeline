
from airflow import DAG
from airflow.decorators import task




def create_portable_task(task_id):
    import os
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    use_aws = os.environ.get("RADIANT_VCF_USE_AWS", "false").lower() == "true"

    if not use_aws:
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        return EcsRunTaskOperator(
            task_id=task_id,
            cluster=os.getenv("AWS_ECS_CLUSTER"),
            task_definition="my-task-def",
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [{
                    "name": "radiant-airflow",
                    "command": [
                        "/home/airflow/.venv/radiant/bin/python", "import_vcf_for_case.py",
                        "--case", "{{ params.case }}"
                    ]
                }]
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": os.environ.get("AWS_ECS_SUBNETS", "").split(","),
                    "assignPublicIp": "ENABLED",
                    "securityGroups": os.environ.get("AWS_ECS_SECURITY_GROUPS", "").split(",")
                }
            },
        )


with DAG(
    dag_id="POC-ecs-k8s-operators",
    schedule_interval=None,
    catchup=False,
    tags=["radiant", "poc", "manual"],
    dag_display_name="[POC] ECS/K8s Operators",
) as dag:

    @task.kubernetes(
        kubernetes_conn_id="kubernetes_conn",
        name="run-on-k8s",
        namespace="radiant",
        image="radiant-k8s-operator:latest",
        image_pull_policy="IfNotPresent",
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        env_vars={
            "PYTHONPATH": "/opt/airflow",
            "LD_LIBRARY_PATH": "/usr/local/lib:$LD_LIBRARY_PATH",
        },
    )
    def run_k8s():
        import logging
        LOGGER = logging.getLogger(__name__)
        for i in range(10):
            LOGGER.warning(f"Running task {i}")
        return [i for i in range(10)]

    run_k8s()
