import datetime
import logging
import os
from collections.abc import Sequence
from itertools import groupby
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from radiant.dags import DEFAULT_ARGS, IS_AWS, NAMESPACE, ECSEnv, get_namespace, load_docs_md
from radiant.tasks.data.radiant_tables import get_iceberg_germline_snv_mapping
from radiant.tasks.starrocks.operator import (
    RadiantLoadExomiserOperator,
    RadiantStarRocksOperator,
    RadiantStarRocksPartitionSwapOperator,
    SubmitTaskOptions,
    SwapPartition,
)
from radiant.tasks.vcf.experiment import (
    ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK,
    RADIANT_GERMLINE_ANNOTATION_TASK,
    build_task_from_rows,
)

if IS_AWS:
    from radiant.dags.operators import ecs as operators
else:
    from radiant.dags.operators import k8s as operators

LOGGER = logging.getLogger(__name__)


PATH_TO_PYTHON_BINARY = os.getenv("RADIANT_PYTHON_PATH", "/home/airflow/.venv/radiant/bin/python")


def tasks_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    column_names = [desc[0] for desc in descriptions[0]]
    dict_rows = sorted([dict(zip(column_names, row, strict=False)) for row in results[0]], key=lambda d: d["task_id"])
    _tasks = [
        build_task_from_rows(list(grouped_rows)).model_dump()
        for task_id, grouped_rows in groupby(dict_rows, key=lambda x: x["task_id"])
    ]
    return [_tasks]


dag_params = {
    "part": Param(
        default=None,
        description="Integer that represents the part that need to be processed.",
    ),
}

std_submit_task_opts = SubmitTaskOptions(max_query_timeout=3600, poll_interval=10)


@dag(
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["radiant", "scheduled"],
    dag_display_name="Radiant - Import for a partition",
    dag_id=f"{NAMESPACE}-import-part",
    description="Processes a single partition of sequencing experiments: imports VCFs, loads exomiser, computes variants/consequences/frequencies, and updates experiment records.",
    params=dag_params,
    render_template_as_native_obj=True,
    doc_md=load_docs_md("import_part.md"),
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def import_part():
    start = EmptyOperator(task_id="start", task_display_name="[Start]")

    fetch_sequencing_experiment_delta = RadiantStarRocksOperator(
        task_id="fetch_sequencing_experiment_delta",
        sql="./sql/radiant/sequencing_experiment_partition_select.sql",
        task_display_name="[StarRocks] Get Sequencing Experiment for a partition",
        doc_md="Fetches sequencing experiment metadata for the given partition from the staging table. Returns rows for experiments that have been updated or deleted.",
        do_xcom_push=True,
        output_processor=tasks_output_processor,
        parameters={"part": "{{ params.part }}"},
    )

    @task.short_circuit(task_id="sanity_check_tasks", task_display_name="[PyOp] Sanity Check Tasks", doc_md="Short-circuits downstream tasks if no tasks were found for this partition.")
    def check_tasks(tasks: Any) -> Any:
        return tasks

    @task(task_id="ecs_store_tasks", task_display_name="[PyOp] ECS Store Tasks", doc_md="Uploads tasks to S3 to work around ECS command override length limits. Returns the S3 path for downstream ECS operators.")
    def ecs_store_tasks(tasks: Any) -> Any:
        from radiant.dags.operators.utils import s3_store_content

        # ECS limits the length of the command override, so we need to upload the tasks to S3
        # and pass the S3 path of the file in which the data is stored to the ECS operator instead of the data.
        s3_path = s3_store_content(content=tasks, ecs_env=ecs_env, prefix="commit_partitions")
        return [{"stored_tasks": s3_path}]

    tasks = check_tasks(fetch_sequencing_experiment_delta.output)
    stored_tasks = ecs_store_tasks(tasks) if IS_AWS else None

    @task(task_id="prepare_config", task_display_name="[PyOp] Prepare Config", doc_md="Prepares the configuration for the germline SNV VCF import sub-DAG by merging the DAG run conf with the list of non-deleted tasks.")
    def prepare_config(tasks: Any) -> Any:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        conf = context["dag_run"].conf or {}
        conf["tasks"] = [t for t in tasks if not t["deleted"]]
        return conf

    _prepare_config = prepare_config(tasks)
    import_vcf = TriggerDagRunOperator(
        task_id="import_germline_snv_vcf",
        task_display_name="[DAG] Import Germline SNV VCF into Iceberg",
        doc_md="Triggers the germline SNV VCF import sub-DAG to ingest VCF files into Iceberg tables. Waits for completion before proceeding.",
        trigger_dag_id=f"{NAMESPACE}-import-germline-snv-vcf",
        conf=_prepare_config,
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=2,
    )

    if IS_AWS:
        ecs_env = ECSEnv()
        import_cnv_vcf = operators.ImportPart.get_import_cnv_vcf(
            radiant_namespace=get_namespace(),
            ecs_env=ecs_env,
        )

    else:
        import_cnv_vcf = operators.ImportPart.get_import_cnv_vcf(get_namespace())

    @task(task_id="extract_seq_ids", task_display_name="[PyOp] Extract Sequencing Experiment IDs", doc_md="Extracts unique sequencing experiment IDs from the tasks, separating active and deleted experiments for downstream filtering.")
    def extract_sequencing_ids(tasks) -> dict[str, list[Any]]:
        seq_ids = []
        deleted_seq_ids = []
        for t in tasks:
            for experiment in t.get("experiments"):
                if experiment:
                    if not t["deleted"]:
                        seq_ids.append(experiment["seq_id"])
                    else:
                        deleted_seq_ids.append(experiment["seq_id"])
        return {"seq_ids": list(set(seq_ids)) or [-1], "deleted_seq_ids": list(set(deleted_seq_ids)) or [-1]}

    sequencing_ids = extract_sequencing_ids(tasks)

    with TaskGroup(group_id="germline_cnv_occurrence") as tg_germline_cnv_occurrence:

        @task.short_circuit(
            task_id="sanity_check_cnvs",
            task_display_name="[PyOp] Sanity Check CNVs",
            doc_md="Short-circuits the CNV occurrence branch if no tasks of type ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK are present in this partition.",
            ignore_downstream_trigger_rules=False,
        )
        def sanity_check_cnvs(tasks: Any) -> Any:
            has_cnv = any(t.get("task_type") == ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK for t in tasks)
            return has_cnv

        insert_germline_cnv_occurrences = RadiantStarRocksPartitionSwapOperator(
            task_id="insert_germline_cnv_occurrences",
            table="{{ mapping.starrocks_germline_cnv_occurrence }}",
            task_display_name="[StarRocks] Insert CNV Occurrences Part",
            doc_md=load_docs_md("task_insert_germline_cnv_occurrences.md"),
            swap_partition=SwapPartition(
                partition="{{ params.part }}",
                copy_partition_sql="./sql/radiant/germline_cnv_occurrence_copy_partition.sql",
            ),
            parameters=sequencing_ids,
            insert_partition_sql="./sql/radiant/germline_cnv_occurrence_insert_partition_delta.sql",
        )

        sanity_check_cnvs(tasks) >> insert_germline_cnv_occurrences

    load_exomiser = RadiantLoadExomiserOperator(
        task_id="load_exomiser_files",
        task_display_name="[StarRocks] Load Exomiser Files",
        doc_md=load_docs_md("task_load_exomiser.md"),
        swap_partition=SwapPartition(
            partition="{{ params.part }}",
            copy_partition_sql="./sql/radiant/staging_exomiser_insert_partition_delta.sql",
        ),
        table="{{ mapping.starrocks_staging_exomiser }}",
        parameters=sequencing_ids,
        tasks=tasks,
    )

    @task(task_id="extract_task_ids", task_display_name="[PyOp] Extract Task IDs", doc_md="Extracts unique task IDs from the tasks, separating active and deleted task IDs for downstream SQL filtering.")
    def extract_task_ids(tasks) -> dict[str, list[Any]]:
        return {
            "task_ids": list(set([t["task_id"] for t in tasks if not t["deleted"]])) or [-1],
            "deleted_task_ids": list(set([t["task_id"] for t in tasks if t["deleted"]])) or [-1],
        }

    @task.short_circuit(
        task_id="sanity_check_delta_snv",
        task_display_name="[PyOp] Sanity Check Delta SNVs",
        doc_md="Short-circuits the SNV variant and consequence branches if no non-deleted tasks of type RADIANT_GERMLINE_ANNOTATION_TASK are present.",
        ignore_downstream_trigger_rules=False,
    )
    def sanity_check_delta_snv(tasks: Any) -> Any:
        has_delta_snv = any(
            t.get("task_type") == RADIANT_GERMLINE_ANNOTATION_TASK and not t.get("deleted") for t in tasks
        )
        return has_delta_snv

    @task(task_id="get_tables_to_refresh", task_display_name="[PyOp] Get list of iceberg tables to refresh", doc_md="Reads the DAG run conf to resolve the Iceberg germline SNV table mapping and returns a list of table names to refresh.")
    def get_tables_to_refresh():
        from airflow.operators.python import get_current_context

        context = get_current_context()
        dag_conf = context["dag_run"].conf or {}

        return [{"table": v} for v in get_iceberg_germline_snv_mapping(dag_conf).values()]

    refresh_iceberg_tables = RadiantStarRocksOperator.partial(
        task_id="refresh_iceberg_tables",
        sql="REFRESH EXTERNAL TABLE {{ params.table }}",
        task_display_name="[StarRocks] Refresh Iceberg Tables",
        doc_md="Refreshes Iceberg external tables in StarRocks so newly ingested VCF data becomes visible. Dynamically expanded over all tables in the germline SNV mapping.",
    ).expand(params=get_tables_to_refresh())

    task_ids = extract_task_ids(tasks)

    insert_hashes = RadiantStarRocksOperator(
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        task_id="insert_variant_hashes",
        sql="./sql/radiant/variant_lookup_insert_hashes.sql",
        task_display_name="[StarRocks] Insert Variants Hashes into Lookup",
        doc_md="Inserts unique locus_hash values into the variant_lookup table for deduplication. Only adds hashes that do not already exist and have valid variant IDs.",
        submit_task_options=std_submit_task_opts,
        parameters=task_ids,
    )

    overwrite_germline_snv_tmp_variants = RadiantStarRocksOperator(
        task_id="overwrite_germline_snv_tmp_variant",
        sql="./sql/radiant/germline_snv_tmp_variant_insert.sql",
        task_display_name="[StarRocks] Insert Germline SNV Variants Tmp tables",
        doc_md="Overwrites the tmp variant table with variant data from Iceberg, enriched with locus IDs from the variant lookup. This table is used as a join target by downstream occurrence, exomiser, and consequence tasks.",
        submit_task_options=std_submit_task_opts,
        parameters=task_ids,
    )

    insert_exomiser = RadiantStarRocksOperator(
        task_id="insert_exomiser",
        sql="./sql/radiant/exomiser_insert.sql",
        task_display_name="[StarRocks] Insert Exomiser",
        doc_md=load_docs_md("task_insert_exomiser.md"),
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    insert_germline_snv_occurrences = RadiantStarRocksPartitionSwapOperator(
        task_id="insert_germline_snv_occurrence",
        table="{{ mapping.starrocks_germline_snv_occurrence }}",
        task_display_name="[StarRocks] Insert Germline SNV Occurrences Part",
        doc_md=load_docs_md("task_insert_germline_snv_occurrences.md"),
        swap_partition=SwapPartition(
            partition="{{ params.part }}",
            copy_partition_sql="./sql/radiant/germline_snv_occurrence_copy_partition.sql",
        ),
        parameters=task_ids,
        insert_partition_sql="./sql/radiant/germline_snv_occurrence_insert_partition_delta.sql",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    insert_stg_germline_snv_variants_freq = RadiantStarRocksOperator(
        task_id="insert_stg_germline_snv_variant_freq",
        sql="./sql/radiant/germline_snv_staging_variant_freq_insert.sql",
        task_display_name="[StarRocks] Insert Stg Germline SNV Variants Freq Part",
        doc_md=load_docs_md("task_germline_snv_variant_frequency.md"),
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    aggregate_germline_snv_variants_frequencies = RadiantStarRocksOperator(
        task_id="aggregate_germline_snv_variant_freq",
        task_display_name="[StarRocks] Aggregate all Germline SNV variants frequencies",
        doc_md=load_docs_md("task_germline_snv_variant_frequency.md"),
        sql="./sql/radiant/germline_snv_variant_frequency_insert.sql",
        submit_task_options=std_submit_task_opts,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    check_delta_snv = sanity_check_delta_snv(tasks)
    with TaskGroup(group_id="germline_snv_variant") as tg_variants:
        insert_germline_snv_staging_variants = RadiantStarRocksOperator(
            task_id="insert_germline_snv_staging_variant",
            task_display_name="[StarRocks] Insert Staging Germline SNV Variants",
            doc_md=load_docs_md("task_germline_snv_variant.md"),
            sql="./sql/radiant/germline_snv_staging_variant_insert.sql",
            submit_task_options=std_submit_task_opts,
        )

        insert_germline_snv_variants_with_freqs = RadiantStarRocksOperator(
            task_id="insert_germline_snv_variant",
            task_display_name="[StarRocks] Insert Germline SNV Variants",
            doc_md=load_docs_md("task_germline_snv_variant.md"),
            sql="./sql/radiant/germline_snv_variant_insert.sql",
            submit_task_options=std_submit_task_opts,
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        @task(task_id="compute_parts", doc_md="Computes the variant partition range from the current part: `variant_part = part // 10`, with `part_lower` and `part_upper` defining the range boundaries.")
        def compute_part(params):
            _magic = 10
            variant_part = int(params["part"]) // _magic
            return {
                "variant_part": variant_part,
                "part_lower": variant_part * _magic,
                "part_upper": (variant_part + 1) * _magic,
            }

        _compute_part = compute_part()

        insert_germline_snv_variants_part = RadiantStarRocksOperator(
            task_id="insert_germline_snv_variant_part",
            sql="./sql/radiant/germline_snv_variant_part_insert_part.sql",
            task_display_name="[StarRocks] Insert Germline SNV Variants Part",
            doc_md=load_docs_md("task_germline_snv_variant.md"),
            submit_task_options=std_submit_task_opts,
            parameters=_compute_part,
        )

        (
            (check_delta_snv >> insert_germline_snv_staging_variants)
            >> (aggregate_germline_snv_variants_frequencies >> insert_germline_snv_variants_with_freqs)
            >> insert_germline_snv_variants_part
        )

    with TaskGroup(group_id="germline_snv_consequence") as tg_consequences:
        import_germline_snv_consequences = RadiantStarRocksOperator(
            task_id="import_germline_snv_consequence",
            sql="./sql/radiant/germline_snv_consequence_insert.sql",
            task_display_name="[StarRocks] Insert Germline SNV Consequences",
            doc_md=load_docs_md("task_germline_snv_consequence.md"),
            submit_task_options=std_submit_task_opts,
            parameters=task_ids,
        )

        import_germline_snv_consequences_filter = RadiantStarRocksOperator(
            task_id="import_germline_snv_consequence_filter",
            sql="./sql/radiant/germline_snv_consequence_filter_insert.sql",
            task_display_name="[StarRocks] Insert Germline SNV Consequences Filter",
            doc_md=load_docs_md("task_germline_snv_consequence.md"),
            submit_task_options=std_submit_task_opts,
        )

        insert_germline_snv_consequences_filter_part = RadiantStarRocksOperator(
            task_id="insert_germline_snv_consequence_filter_part",
            sql="./sql/radiant/germline_snv_consequence_filter_insert_part.sql",
            task_display_name="[StarRocks] Insert Germline SNV Consequences Filter Part",
            doc_md=load_docs_md("task_germline_snv_consequence.md"),
            submit_task_options=std_submit_task_opts,
            parameters={"part": "{{ params.part }}"},
        )

        (
            import_germline_snv_consequences
            >> import_germline_snv_consequences_filter
            >> insert_germline_snv_consequences_filter_part
        )
    delete_sequencing_experiments = RadiantStarRocksOperator(
        task_id="delete_sequencing_experiments",
        sql="./sql/radiant/sequencing_experiment_delete.sql",
        task_display_name="[Starrocks] Delete Sequencing Experiments",
        doc_md="Deletes sequencing experiment records from the staging table for deleted task IDs.",
        parameters=task_ids,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    update_sequencing_experiments = RadiantStarRocksOperator(
        task_id="update_sequencing_experiment",
        sql="./sql/radiant/sequencing_experiment_update.sql",
        task_display_name="[Starrocks] Update Sequencing Experiments",
        doc_md="Marks processed task IDs with the current timestamp in the `ingested_at` field of the staging sequencing experiment table.",
        parameters=task_ids,
    )

    (
        start
        >> fetch_sequencing_experiment_delta
        >> (
            import_vcf,
            import_cnv_vcf.expand(params=stored_tasks) if IS_AWS else import_cnv_vcf(tasks=tasks),
        )
        >> load_exomiser
        >> refresh_iceberg_tables
        >> tg_germline_cnv_occurrence
        >> (check_delta_snv >> insert_hashes)
        >> overwrite_germline_snv_tmp_variants
        >> (load_exomiser >> insert_exomiser)
        >> (refresh_iceberg_tables >> insert_germline_snv_occurrences)
        >> insert_stg_germline_snv_variants_freq
        >> aggregate_germline_snv_variants_frequencies
        >> tg_variants
        >> (check_delta_snv >> tg_consequences)
        >> ((tg_consequences, tg_variants) >> delete_sequencing_experiments)
        >> update_sequencing_experiments
    )


import_part()
