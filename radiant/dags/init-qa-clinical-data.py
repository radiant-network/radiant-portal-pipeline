from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from radiant.dags import DEFAULT_ARGS, NAMESPACE
from radiant.tasks.data.radiant_tables import get_radiant_mapping

with DAG(
    dag_id=f"{NAMESPACE}-init-simulated-clinical-data",
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["radiant", "postgres", "manual", "qa"],
    dag_display_name="[QA] Radiant - Init Simulated Clinical Data",
) as dag:
    _mapping = get_radiant_mapping()
    _mapping = {key: value.replace("radiant_jdbc", "radiant").replace("`", "") for key, value in _mapping.items()}

    # PostgreSQL Operator
    init_data = SQLExecuteQueryOperator(
        conn_id="postgres_conn",
        task_id="init_clinical_data",
        task_display_name="[PG] Init Clinical Data",
        sql="./sql/clinical/seeds.sql",
        params=_mapping,
    )
