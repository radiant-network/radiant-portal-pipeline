import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from radiant.dags import NAMESPACE
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions

LOGGER = logging.getLogger(__name__)


default_args = {"owner": "radiant"}
variant_group_ids = ["1000_genomes", "clinvar", "dbnsfp", "gnomad", "spliceai", "topmed_bravo"]
gene_group_ids = [
    "gnomad_constraint",
    "omim_gene_panel",
    "hpo_gene_panel",
    "orphanet_gene_panel",
    "ddd_gene_panel",
    "cosmic_gene_panel",
]

dag_params = {
    "raw_rcv_filepaths": Param(
        default=None,
        description="RCV filepaths to load into the raw ClinVar RCV Summary table.",
        type=["array", "null"],
    ),
}

with DAG(
    dag_id=f"{NAMESPACE}-import-open-data",
    dag_display_name="Radiant - Import Open Data",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    params=dag_params,
    render_template_as_native_obj=True,
    tags=["radiant", "starrocks", "open-data", "manual"],
) as dag:
    start = EmptyOperator(task_id="start")

    data_tasks = []
    for group in variant_group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_hashes_{group}",
                task_display_name=f"{group} Insert Hashes",
                sql=f"./sql/open_data/{group}_insert_hashes.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                trigger_rule="none_failed",
            )
        )
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            )
        )

    for group in gene_group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            )
        )

    # TODO : Import RCV files here in a Python operator
    @task(task_id="load_raw_clinvar_rcv_summary", task_display_name="[PyOp] Load Raw ClinVar RCV Summary")
    def load_raw_clinvar_rcv_summary(filepaths) -> None:
        LOGGER.warning(f"Processing RCV filepaths: {filepaths}")
        if not filepaths:
            LOGGER.warning("No RCV filepaths provided, skipping load.")
            return

        import os
        import time
        import uuid

        import jinja2
        from airflow.hooks.base import BaseHook

        from radiant.dags import DAGS_DIR
        from radiant.tasks.data.radiant_tables import get_radiant_mapping

        conn = BaseHook.get_connection("starrocks_conn")
        _query_params = get_radiant_mapping() | {"broker_load_timeout": 7200}

        _truncate_sql = """
        TRUNCATE TABLE {{ params.starrocks_raw_clinvar_rcv_summary }};
        """
        _prepared_truncate_sql = jinja2.Template(_truncate_sql).render({"params": _query_params})

        _path = os.path.join(DAGS_DIR.resolve(), "sql/open_data/raw_clinvar_rcv_summary_load.sql")
        with open(_path) as f_in:
            raw_clinvar_rcv_summary_load_sql = jinja2.Template(f_in.read()).render({"params": _query_params})

        if os.getenv("STARROCKS_BROKER_USE_INSTANCE_PROFILE", "false").lower() == "true":
            broker_configuration = f"""
                'aws.s3.use_instance_profile' = 'true',
                'aws.s3.region' = '{os.getenv("AWS_REGION", "us-east-1")}'
            """
        else:
            broker_configuration = f"""
                'aws.s3.region' = '{os.getenv("AWS_REGION", "us-east-1")}',
                'aws.s3.endpoint' = '{os.getenv("AWS_ENDPOINT_URL", "s3.amazonaws.com")}',
                'aws.s3.enable_path_style_access' = 'true',
                'aws.s3.access_key' = '{os.getenv("AWS_ACCESS_KEY_ID", "access_key")}',
                'aws.s3.secret_key' = '{os.getenv("AWS_SECRET_ACCESS_KEY", "secret_key")}'
            """

        _label = f"raw_clinvar_rcv_summary_load_{str(uuid.uuid4().hex)}"
        _prepared_load_sql = raw_clinvar_rcv_summary_load_sql.format(
            label=_label,
            broker_configuration=broker_configuration,
            database_name=conn.schema,
        )

        with conn.get_hook().get_conn().cursor() as cursor:
            LOGGER.warning(f"Truncating raw ClinVar RCV Summary table. \nSQL:\n {_prepared_truncate_sql}")
            cursor.execute(_prepared_truncate_sql)

            LOGGER.warning(f"Loading raw ClinVar RCV Summary data. SQL:\n {_prepared_load_sql}")
            cursor.execute(_prepared_load_sql, {"filepaths": filepaths})

            _i = 0
            while True:
                cursor.execute(f"SELECT STATE FROM information_schema.loads WHERE LABEL = '{_label}'")
                load_state = cursor.fetchone()
                LOGGER.info(f"Load state for label {_label}: {load_state}")
                if not load_state or load_state[0] == "FINISHED":
                    break
                if load_state[0] == "CANCELLED":
                    raise RuntimeError(f"Load for label {_label} was cancelled.")
                time.sleep(2)
                _i += 1
                if _i > 30:
                    raise TimeoutError(f"Load for label {_label} did not finish in time.")

    insert_clinvar_rcv_summary = RadiantStarRocksOperator(
        task_id="insert_clinvar_rcv_summary",
        task_display_name="[StarRocks] ClinVar RCV Summary Insert Data",
        sql="./sql/open_data/clinvar_rcv_summary_insert.sql",
        submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
    )

    chain(
        start,
        *data_tasks,
        load_raw_clinvar_rcv_summary(filepaths="{{ params.raw_rcv_filepaths }}"),
        insert_clinvar_rcv_summary,
    )
