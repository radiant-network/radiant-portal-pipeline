import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

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
    "ensembl_exon_by_gene",
    "orphanet_gene_panel",
    "ddd_gene_panel",
    "cosmic_gene_panel",
    "mondo_term",
    "hpo_term",
]

# The `mapping.iceberg_*` key each group reads, for the six that are not simply `iceberg_{group}`.
source_keys = {
    "gnomad": "iceberg_gnomad_joint",
    "cosmic_gene_panel": "iceberg_cosmic_gene_set",
    "ddd_gene_panel": "iceberg_ddd_gene_set",
    "hpo_gene_panel": "iceberg_hpo_gene_set",
    "omim_gene_panel": "iceberg_omim_gene_set",
    "orphanet_gene_panel": "iceberg_orphanet_gene_set",
}


def skip_legacy(group: str) -> str:
    key = source_keys.get(group, f"iceberg_{group}")
    return f"{{{{ params.skip_legacy_tables and not mapping.get('{key}_is_contract') }}}}"


dag_params = {
    "skip_legacy_tables": Param(
        default=False,
        description=(
            "Import only the OpenDataLake sources, skipping every table still read from the legacy Radiant "
            "Iceberg catalog. Those do not move when OpenDataLake publishes, so a refresh-driven run has "
            "nothing new to read for them. Set by the re-annotation DAG; leave False for a full import."
        ),
        type="boolean",
    ),
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
    schedule=None,
    catchup=False,
    default_args=default_args,
    params=dag_params,
    render_template_as_native_obj=True,
    tags=["radiant", "starrocks", "open-data", "manual"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="get_tables_to_refresh", task_display_name="[PyOp] Iceberg Tables to Refresh")
    def get_tables_to_refresh() -> list[dict[str, str]]:
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.open_data import list_iceberg_source_tables

        conf = get_current_context()["dag_run"].conf or {}
        return [{"table": table} for table in list_iceberg_source_tables(conf)]

    _tables_to_refresh = get_tables_to_refresh()

    refresh_iceberg_tables = RadiantStarRocksOperator.partial(
        task_id="refresh_iceberg_tables",
        task_display_name="[StarRocks] Refresh Iceberg Metadata Cache",
        sql="REFRESH EXTERNAL TABLE {{ params.table }}",
        map_index_template="{{ params.table }}",
    ).expand(params=_tables_to_refresh)

    data_tasks = []
    for group in variant_group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_hashes_{group}",
                task_display_name=f"{group} Insert Hashes",
                sql=f"./sql/open_data/{group}_insert_hashes.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                trigger_rule="none_failed",
                skip_if=skip_legacy(group),
            )
        )
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                trigger_rule="none_failed",
                skip_if=skip_legacy(group),
            )
        )

    for group in gene_group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                trigger_rule="none_failed",
                skip_if=skip_legacy(group),
            )
        )

    @task.short_circuit(
        task_id="has_raw_rcv_filepaths",
        task_display_name="[PyOp] RCV Summary Filepaths Provided?",
        ignore_downstream_trigger_rules=False,
    )
    def has_raw_rcv_filepaths(params: dict | None = None) -> bool:
        return bool((params or {}).get("raw_rcv_filepaths"))

    @task.short_circuit(
        task_id="has_cytoband_filepath",
        task_display_name="[PyOp] Cytoband Filepath Provided?",
        ignore_downstream_trigger_rules=False,
    )
    def has_cytoband_filepath(params: dict | None = None) -> bool:
        return bool((params or {}).get("cytoband_filepath"))

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

    start >> _tables_to_refresh
    chain(refresh_iceberg_tables, *data_tasks)

    data_tasks[-1] >> has_raw_rcv_filepaths() >> load_raw_clinvar_rcv_summary >> insert_clinvar_rcv_summary
    data_tasks[-1] >> has_cytoband_filepath() >> load_cytoband
