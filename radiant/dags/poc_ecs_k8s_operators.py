from airflow import DAG
from airflow.decorators import task

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
        image="radiant-vcf-operator:latest",
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
