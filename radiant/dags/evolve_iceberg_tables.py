import logging

import pendulum
from airflow import DAG

from radiant.dags import IS_AWS, NAMESPACE, ECSEnv, get_namespace

default_args = {
    "owner": "radiant",
}

LOGGER = logging.getLogger(__name__)

# Every Iceberg table the pipeline writes. Each is evolved independently — there is no
# ordering between them, unlike the create DAG where the database must exist first.
ICEBERG_TABLES = (
    "snv_variant",
    "snv_consequence",
    "germline_snv_occurrence",
    "germline_cnv_occurrence",
    "somatic_snv_occurrence",
)

with DAG(
    dag_id=f"{NAMESPACE}-evolve-iceberg-tables",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["radiant", "iceberg", "manual"],
    dag_display_name="Radiant - Evolve Iceberg Tables",
    doc_md="""
    Adds any column missing from the existing Iceberg tables, in place.

    This is the non-destructive counterpart to `Radiant - Init Iceberg Tables`, which drops
    and recreates every table. Run this one after a schema addition to pick the new columns
    up without losing the staged data. Iceberg applies it as a metadata-only change:
    existing Parquet files are untouched and read NULL for the added columns.

    Only additive changes are supported. Dropping a column, narrowing a type or making a
    column required still requires the init DAG.
    """,
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

        for table in ICEBERG_TABLES:
            namespace_task >> ecs.InitIcebergTables.get_init_iceberg(namespace_task, table, ecs_env, mode="evolve")

    else:
        try:
            from radiant.dags.operators import k8s
        except ImportError as ie:
            LOGGER.error("Kubernetes provider not found. Please install the required provider.")
            raise ie

        for table in ICEBERG_TABLES:
            namespace_task >> k8s.InitIcebergTables.get_evolve_iceberg_table(namespace_task, table)(table_name=table)
