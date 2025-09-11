import logging

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from radiant.dags import NAMESPACE
from radiant.tasks.starrocks.operator import RadiantStarrocksLoadOperator, RadiantStarRocksOperator, SubmitTaskOptions

LOGGER = logging.getLogger(__name__)


default_args = {"owner": "radiant"}
variant_group_ids = ["1000_genomes", "clinvar", "dbnsfp", "dbsnp", "gnomad", "spliceai", "topmed_bravo"]
gene_group_ids = [
    "gnomad_constraint",
    "omim_gene_panel",
    "hpo_gene_panel",
    "ensembl_gene",
    "orphanet_gene_panel",
    "ddd_gene_panel",
    "cosmic_gene_panel",
    "mondo_term",
    "hpo_term",
]

dag_params = {
    "raw_rcv_filepaths": Param(
        default=None,
        description="RCV filepaths to load into the raw ClinVar RCV Summary table.",
        type=["array", "null"],
    ),
    "cytoband_filepath": Param(
        default=None,
        description="Cytoband filepath to load into the raw Cytoband table.",
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

    load_raw_clinvar_rcv_summary = RadiantStarrocksLoadOperator(
        task_id="load_raw_clinvar_rcv_summary",
        task_display_name="[StarRocks] Load Raw ClinVar RCV Summary",
        sql="./sql/open_data/raw_clinvar_rcv_summary_load.sql",
        table="{{ mapping.starrocks_raw_clinvar_rcv_summary }}",
        truncate=True,
        load_label="load_raw_clinvar_rcv_summary_{{ ts_nodash }}_{{ ti.try_number }}",
        parameters={"rcv_summary_filepaths": "{{ params.raw_rcv_filepaths }}"},
    )

    insert_clinvar_rcv_summary = RadiantStarRocksOperator(
        task_id="insert_clinvar_rcv_summary",
        task_display_name="[StarRocks] ClinVar RCV Summary Insert Data",
        sql="./sql/open_data/clinvar_rcv_summary_insert.sql",
        submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
    )

    load_cytoband = RadiantStarrocksLoadOperator(
        task_id="load_cytoband",
        task_display_name="[StarRocks] Load Cytoband",
        sql="./sql/open_data/cytoband_load.sql",
        table="{{ mapping.starrocks_cytoband }}",
        truncate=True,
        load_label="load_cytoband_{{ ts_nodash }}_{{ ti.try_number }}",
        parameters={"tsv_filepath": "{{ params.cytoband_filepath }}"},
    )

    chain(
        start,
        *data_tasks,
        load_raw_clinvar_rcv_summary,
        insert_clinvar_rcv_summary,
        load_cytoband
    )
