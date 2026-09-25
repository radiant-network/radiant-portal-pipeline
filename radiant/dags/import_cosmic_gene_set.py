import logging

from airflow import DAG
from airflow.models import Param

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md
from radiant.tasks.starrocks.operator import RadiantStarrocksLoadOperator, RadiantStarRocksOperator, SubmitTaskOptions

LOGGER = logging.getLogger(__name__)

dag_params = {
    "cosmic_gene_set_filepath": Param(
        type="array",
        items={"type": "string"},
        title="COSMIC Cancer Gene Census file(s)",
        description=(
            "S3 path(s) of the COSMIC Cancer Gene Census TSV (Cosmic_CancerGeneCensus_GRCh38.tsv.gz) "
            "to load into cosmic_gene_set. The table is truncated first, so this is a full replace."
        ),
    ),
}

with DAG(
    dag_id=f"{NAMESPACE}-import-cosmic-gene-set",
    dag_display_name="Radiant - Import COSMIC Gene Set",
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=dag_params,
    render_template_as_native_obj=True,
    tags=["radiant", "starrocks", "open-data", "manual"],
    doc_md=load_docs_md("import_cosmic_gene_set.md"),
) as dag:
    load_cosmic_gene_set = RadiantStarrocksLoadOperator(
        task_id="load_cosmic_gene_set",
        task_display_name="[StarRocks] Load COSMIC Gene Set",
        sql="./sql/open_data/cosmic_gene_set_load.sql",
        table="{{ mapping.starrocks_cosmic_gene_set }}",
        truncate=True,
        load_label="load_cosmic_gene_set_{{ ts_nodash }}_{{ ti.try_number }}",
        parameters={"tsv_filepath": "{{ params.cosmic_gene_set_filepath }}"},
    )

    insert_cosmic_gene_panel = RadiantStarRocksOperator(
        task_id="insert_cosmic_gene_panel",
        task_display_name="[StarRocks] COSMIC Gene Panel Insert Data",
        sql="./sql/open_data/cosmic_gene_panel_insert.sql",
        submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
    )

    load_cosmic_gene_set >> insert_cosmic_gene_panel
