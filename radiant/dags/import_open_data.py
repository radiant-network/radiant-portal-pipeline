import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

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
    "mondo_term",
    "hpo_term",
]

# The `mapping.iceberg_*` key each group reads, for the five that are not simply `iceberg_{group}`.
source_keys = {
    "gnomad": "iceberg_gnomad_joint",
    "ddd_gene_panel": "iceberg_ddd_gene_set",
    "hpo_gene_panel": "iceberg_hpo_gene_set",
    "omim_gene_panel": "iceberg_omim_gene_set",
    "orphanet_gene_panel": "iceberg_orphanet_gene_set",
}


IMPORT_GATES_TASK_ID = "compute_import_gates"


def skip_legacy(group: str) -> str:
    """Clause one: the source lives on the legacy catalog and the caller asked for contracts only."""
    key = source_keys.get(group, f"iceberg_{group}")
    return f"params.skip_legacy_tables and not mapping.get('{key}_is_contract')"


def skip_unchanged(group: str) -> str:
    """Clause two: OpenDataLake has published nothing since the copy in StarRocks was loaded.

    Contract sources only. A held-back source is read off the legacy catalog without time travel,
    so it has no ref, no snapshot, and nothing that could report it as moved -- gating it here would
    mean never importing it again after the first run.

    The gate task returns True for a source with something new, so the skip is its negation, and a
    source missing from the map defaults to True: unknown means import.
    """
    key = source_keys.get(group, f"iceberg_{group}")
    # `not not` is what keeps the clause a bool: `_is_contract` is the string "true" or "", so a bare
    # `mapping.get(...) and ...` renders "" rather than False for a held-back source.
    return (
        f"not not mapping.get('{key}_is_contract') and not params.force_import "
        f"and not (ti.xcom_pull(task_ids='{IMPORT_GATES_TASK_ID}') or {{}}).get('{key}', True)"
    )


def gated(group: str) -> str:
    """The full `skip_if` for both statements of one group. Renders to a real bool -- the DAG sets
    `render_template_as_native_obj`."""
    return "{{ (" + skip_legacy(group) + ") or (" + skip_unchanged(group) + ") }}"


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
    "force_import": Param(
        default=False,
        description=(
            "Re-import every source whether or not OpenDataLake published since the last recorded "
            "release. Set it after editing an insert statement, and after truncating or recreating a "
            "StarRocks open-data table -- the gate compares Iceberg snapshots and cannot see either."
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
    "cosmic_gene_set_filepath": Param(
        default=None,
        description=(
            "COSMIC Cancer Gene Census filepath(s). When set, triggers radiant-import-cosmic-gene-set, which "
            "loads cosmic_gene_set and rebuilds cosmic_gene_panel from it."
        ),
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

    @task(task_id=IMPORT_GATES_TASK_ID, task_display_name="[PyOp] Which Sources Have New Data?")
    def compute_import_gates() -> dict[str, bool]:
        """One entry per open-data source, True when OpenDataLake has published since the last
        recorded release.

        Sits after `refresh_iceberg_tables` so the `$refs` read is not answered from metadata this
        run has already superseded.
        """
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.open_data import changed_sources, resolve_iceberg_source_tables

        conf = get_current_context()["dag_run"].conf or {}
        changed = changed_sources(conf)
        gates = {key: key in changed for key in resolve_iceberg_source_tables(conf)}
        LOGGER.info(f"Sources with something new: {sorted(changed) or 'none'}.")
        return gates

    _import_gates = compute_import_gates()

    data_tasks = []
    for group in variant_group_ids:
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_hashes_{group}",
                task_display_name=f"{group} Insert Hashes",
                sql=f"./sql/open_data/{group}_insert_hashes.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                trigger_rule="none_failed",
                skip_if=gated(group),
            )
        )
        data_tasks.append(
            RadiantStarRocksOperator(
                task_id=f"insert_{group}",
                task_display_name=f"{group} Insert Data",
                sql=f"./sql/open_data/{group}_insert.sql",
                submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
                trigger_rule="none_failed",
                skip_if=gated(group),
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
                skip_if=gated(group),
            )
        )

    @task.short_circuit(
        task_id="has_cytoband_filepath",
        task_display_name="[PyOp] Cytoband Filepath Provided?",
        ignore_downstream_trigger_rules=False,
    )
    def has_cytoband_filepath(params: dict | None = None) -> bool:
        return bool((params or {}).get("cytoband_filepath"))

    with TaskGroup(group_id="clinvar_rcv_summary") as tg_clinvar_rcv_summary:

        @task.short_circuit(
            task_id="has_raw_filepaths",
            task_display_name="[PyOp] RCV Summary Filepaths Provided?",
            ignore_downstream_trigger_rules=False,
            trigger_rule="none_failed",
        )
        def has_raw_filepaths(params: dict | None = None) -> bool:
            return bool((params or {}).get("raw_rcv_filepaths"))

        insert_raw_from_open_data = RadiantStarRocksOperator(
            task_id="insert_raw_from_open_data",
            task_display_name="[StarRocks] Raw ClinVar RCV Summary Insert Data",
            sql="./sql/open_data/raw_clinvar_rcv_summary_insert.sql",
            submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            trigger_rule="none_failed",
            # Not `gated()`: clinvar_rcv has no pre-contract table, so a held-back run has nothing to
            # read at all and must skip whatever `skip_legacy_tables` says. The snapshot half is the
            # same as every other group's.
            skip_if=(
                f"{{{{ (not mapping.get('iceberg_clinvar_rcv_is_contract')) or ({skip_unchanged('clinvar_rcv')}) }}}}"
            ),
        )

        load_raw_from_files = RadiantStarrocksLoadOperator(
            task_id="load_raw_from_files",
            task_display_name="[StarRocks] Load Raw ClinVar RCV Summary",
            sql="./sql/open_data/raw_clinvar_rcv_summary_load.sql",
            table="{{ mapping.starrocks_raw_clinvar_rcv_summary }}",
            truncate=True,
            load_label="load_raw_clinvar_rcv_summary_{{ ts_nodash }}_{{ ti.try_number }}",
            parameters={"rcv_summary_filepaths": "{{ params.raw_rcv_filepaths }}"},
        )

        insert_summary = RadiantStarRocksOperator(
            task_id="insert_summary",
            task_display_name="[StarRocks] ClinVar RCV Summary Insert Data",
            sql="./sql/open_data/clinvar_rcv_summary_insert.sql",
            submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=30),
            trigger_rule="none_failed",
        )

        insert_raw_from_open_data >> has_raw_filepaths() >> load_raw_from_files >> insert_summary

    load_cytoband = RadiantStarrocksLoadOperator(
        task_id="load_cytoband",
        task_display_name="[StarRocks] Load Cytoband",
        sql="./sql/open_data/cytoband_load.sql",
        table="{{ mapping.starrocks_cytoband }}",
        truncate=True,
        load_label="load_cytoband_{{ ts_nodash }}_{{ ti.try_number }}",
        parameters={"tsv_filepath": "{{ params.cytoband_filepath }}"},
    )

    # COSMIC has no OpenDataLake contract and is not an Iceberg source any more: it is loaded from the
    # census TSV by its own DAG, which also rebuilds cosmic_gene_panel.
    with TaskGroup(group_id="cosmic_gene_set") as tg_cosmic_gene_set:

        @task.short_circuit(
            task_id="has_cosmic_gene_set_filepath",
            task_display_name="[PyOp] COSMIC Gene Set Filepath Provided?",
            ignore_downstream_trigger_rules=False,
        )
        def has_cosmic_gene_set_filepath(params: dict | None = None) -> bool:
            return bool((params or {}).get("cosmic_gene_set_filepath"))

        trigger_import_cosmic_gene_set = TriggerDagRunOperator(
            task_id="trigger_import_cosmic_gene_set",
            task_display_name="[DAG] Import COSMIC Gene Set",
            trigger_dag_id=f"{NAMESPACE}-import-cosmic-gene-set",
            conf={"cosmic_gene_set_filepath": "{{ params.cosmic_gene_set_filepath }}"},
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=30,
        )

        has_cosmic_gene_set_filepath() >> trigger_import_cosmic_gene_set

    start >> _tables_to_refresh
    chain(refresh_iceberg_tables, _import_gates, *data_tasks)

    data_tasks[-1] >> tg_clinvar_rcv_summary
    data_tasks[-1] >> has_cytoband_filepath() >> load_cytoband
    data_tasks[-1] >> tg_cosmic_gene_set
