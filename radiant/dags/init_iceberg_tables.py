import logging
import os

from airflow import DAG
from airflow.utils.dates import days_ago

from radiant.dags import IS_AWS, NAMESPACE, ECSEnv, get_namespace

default_args = {
    "owner": "radiant",
}

PATH_TO_PYTHON_BINARY = os.getenv("RADIANT_PYTHON_PATH", "/home/airflow/.venv/radiant/bin/python")

LOGGER = logging.getLogger(__name__)

with DAG(
    dag_id=f"{NAMESPACE}-init-iceberg-tables",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["radiant", "iceberg", "manual"],
    dag_display_name="Radiant - Init Iceberg Tables",
    catchup=False,
) as dag:
    namespace_task = get_namespace()

    if IS_AWS:
        ecs_env = ECSEnv()

        try:
            from radiant.dags.operators import ecs
        except ImportError as ie:
            LOGGER.error("ECS provider not found. Please install the required provider.")
            raise ie

        init_database = ecs.InitIcebergTables.get_init_iceberg(namespace_task, "database", ecs_env)
        create_germline_snv_occurrence_table = ecs.InitIcebergTables.get_init_iceberg(
            namespace_task, "germline_snv_occurrence", ecs_env
        )
        create_germline_variant_table = ecs.InitIcebergTables.get_init_iceberg(
            namespace_task, "germline_variant", ecs_env
        )
        create_germline_consequence_table = ecs.InitIcebergTables.get_init_iceberg(
            namespace_task, "germline_consequence", ecs_env
        )
        create_germline_cnv_occurrence_table = ecs.InitIcebergTables.get_init_iceberg(
            namespace_task, "germline_cnv_occurrence", ecs_env
        )

    else:
        try:
            from radiant.dags.operators import k8s
        except ImportError as ie:
            LOGGER.error("Kubernetes provider not found. Please install the required provider.")
            raise ie

        init_database = k8s.InitIcebergTables.get_init_database(namespace_task)
        create_germline_snv_occurrence_table = k8s.InitIcebergTables.get_create_germline_snv_occurrence_table(
            namespace_task
        )
        create_germline_variant_table = k8s.InitIcebergTables.get_create_germline_variant_table(namespace_task)
        create_germline_consequence_table = k8s.InitIcebergTables.get_create_germline_consequence_table(namespace_task)
        create_germline_cnv_occurrence_table = k8s.InitIcebergTables.get_create_germline_cnv_occurrence_table(
            namespace_task
        )

        (
            init_database()
            >> create_germline_snv_occurrence_table()
            >> create_germline_variant_table()
            >> create_germline_consequence_table()
            >> create_germline_cnv_occurrence_table()
        )
