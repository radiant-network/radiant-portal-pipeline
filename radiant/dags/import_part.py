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
from radiant.tasks.vcf.experiment import ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK, build_task_from_rows

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
        do_xcom_push=True,
        output_processor=tasks_output_processor,
        parameters={"part": "{{ params.part }}"},
    )

    @task.short_circuit(task_id="sanity_check_tasks", task_display_name="[PyOp] Sanity Check Tasks")
    def check_tasks(tasks: Any) -> Any:
        return tasks

    @task(task_id="ecs_store_tasks", task_display_name="[PyOp] ECS Store Tasks")
    def ecs_store_tasks(tasks: Any) -> Any:
        from radiant.dags.operators.utils import s3_store_content

        # ECS limits the length of the command override, so we need to upload the tasks to S3
        # and pass the S3 path of the file in which the data is stored to the ECS operator instead of the data.
        s3_path = s3_store_content(content=tasks, ecs_env=ecs_env, prefix="commit_partitions")
        return [{"stored_tasks": s3_path}]

    tasks = check_tasks(fetch_sequencing_experiment_delta.output)
    stored_tasks = ecs_store_tasks(tasks) if IS_AWS else None

    @task(task_id="prepare_config", task_display_name="[PyOp] Prepare Config")
    def prepare_config(tasks: Any) -> Any:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        conf = context["dag_run"].conf or {}
        conf["tasks"] = tasks
        return conf

    _prepare_config = prepare_config(tasks)
    import_vcf = TriggerDagRunOperator(
        task_id="import_germline_snv_vcf",
        task_display_name="[DAG] Import Germline SNV VCF into Iceberg",
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

    @task(task_id="extract_seq_ids", task_display_name="[PyOp] Extract Sequencing Experiment IDs")
    def extract_sequencing_ids(tasks) -> dict[str, list[Any]]:
        seq_ids = []
        for t in tasks:
            for experiment in t.get("experiments"):
                if experiment:
                    seq_ids.append(experiment["seq_id"])
        return {"seq_ids": seq_ids}

    sequencing_ids = extract_sequencing_ids(tasks)

    with TaskGroup(group_id="germline_cnv_occurrence") as tg_germline_cnv_occurrence:

        @task.short_circuit(
            task_id="sanity_check_cnvs",
            task_display_name="[PyOp] Sanity Check CNVs",
            ignore_downstream_trigger_rules=False,
        )
        def sanity_check_cnvs(tasks: Any) -> Any:
            has_cnv = any(t.get("task_type") == ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK for t in tasks)
            return has_cnv

        insert_germline_cnv_occurrences = RadiantStarRocksPartitionSwapOperator(
            task_id="insert_germline_cnv_occurrences",
            table="{{ mapping.starrocks_germline_cnv_occurrence }}",
            task_display_name="[StarRocks] Insert CNV Occurrences Part",
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
        swap_partition=SwapPartition(
            partition="{{ params.part }}",
            copy_partition_sql="./sql/radiant/staging_exomiser_insert_partition_delta.sql",
        ),
        table="{{ mapping.starrocks_staging_exomiser }}",
        parameters=sequencing_ids,
        tasks=tasks,
    )

    @task(task_id="extract_task_ids", task_display_name="[PyOp] Extract Task IDs")
    def extract_task_ids(tasks) -> dict[str, list[Any]]:
        return {"task_ids": [t["task_id"] for t in tasks]}

    @task(task_id="get_tables_to_refresh", task_display_name="[PyOp] Get list of iceberg tables to refresh")
    def get_tables_to_refresh():
        from airflow.operators.python import get_current_context

        context = get_current_context()
        dag_conf = context["dag_run"].conf or {}

        return [{"table": v} for v in get_iceberg_germline_snv_mapping(dag_conf).values()]

    refresh_iceberg_tables = RadiantStarRocksOperator.partial(
        task_id="refresh_iceberg_tables",
        sql="REFRESH EXTERNAL TABLE {{ params.table }}",
        task_display_name="[StarRocks] Refresh Iceberg Tables",
    ).expand(params=get_tables_to_refresh())

    task_ids = extract_task_ids(tasks)

    insert_hashes = RadiantStarRocksOperator(
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        task_id="insert_variant_hashes",
        sql="./sql/radiant/variant_lookup_insert_hashes.sql",
        task_display_name="[StarRocks] Insert Variants Hashes into Lookup",
        submit_task_options=std_submit_task_opts,
        parameters=task_ids,
    )

    overwrite_germline_snv_tmp_variants = RadiantStarRocksOperator(
        task_id="overwrite_germline_snv_tmp_variant",
        sql="./sql/radiant/germline_snv_tmp_variant_insert.sql",
        task_display_name="[StarRocks] Insert Germline SNV Variants Tmp tables",
        submit_task_options=std_submit_task_opts,
        parameters=task_ids,
    )

    insert_exomiser = RadiantStarRocksOperator(
        task_id="insert_exomiser",
        sql="./sql/radiant/exomiser_insert.sql",
        task_display_name="[StarRocks] Insert Exomiser",
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
    )

    insert_germline_snv_occurrences = RadiantStarRocksPartitionSwapOperator(
        task_id="insert_germline_snv_occurrence",
        table="{{ mapping.starrocks_germline_snv_occurrence }}",
        task_display_name="[StarRocks] Insert Germline SNV Occurrences Part",
        swap_partition=SwapPartition(
            partition="{{ params.part }}",
            copy_partition_sql="./sql/radiant/germline_snv_occurrence_copy_partition.sql",
        ),
        parameters=task_ids,
        insert_partition_sql="./sql/radiant/germline_snv_occurrence_insert_partition_delta.sql",
    )

    insert_stg_germline_snv_variants_freq = RadiantStarRocksOperator(
        task_id="insert_stg_germline_snv_variant_freq",
        sql="./sql/radiant/germline_snv_staging_variant_freq_insert.sql",
        task_display_name="[StarRocks] Insert Stg Germline SNV Variants Freq Part",
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
    )

    aggregate_germline_snv_variants_frequencies = RadiantStarRocksOperator(
        task_id="aggregate_germline_snv_variant_freq",
        task_display_name="[StarRocks] Aggregate all Germline SNV variants frequencies",
        sql="./sql/radiant/germline_snv_variant_frequency_insert.sql",
        submit_task_options=std_submit_task_opts,
    )

    with TaskGroup(group_id="germline_snv_variant") as tg_variants:
        insert_germline_snv_staging_variants = RadiantStarRocksOperator(
            task_id="insert_germline_snv_staging_variant",
            task_display_name="[StarRocks] Insert Staging Germline SNV Variants",
            sql="./sql/radiant/germline_snv_staging_variant_insert.sql",
            submit_task_options=std_submit_task_opts,
        )

        insert_germline_snv_variants_with_freqs = RadiantStarRocksOperator(
            task_id="insert_germline_snv_variant",
            task_display_name="[StarRocks] Insert Germline SNV Variants",
            sql="./sql/radiant/germline_snv_variant_insert.sql",
            submit_task_options=std_submit_task_opts,
        )

        @task(task_id="compute_parts")
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
            submit_task_options=std_submit_task_opts,
            parameters=_compute_part,
        )

        (
            insert_germline_snv_staging_variants
            >> insert_germline_snv_variants_with_freqs
            >> insert_germline_snv_variants_part
        )

    with TaskGroup(group_id="germline_snv_consequence") as tg_consequences:
        import_germline_snv_consequences = RadiantStarRocksOperator(
            task_id="import_germline_snv_consequence",
            sql="./sql/radiant/germline_snv_consequence_insert.sql",
            task_display_name="[StarRocks] Insert Germline SNV Consequences",
            submit_task_options=std_submit_task_opts,
            parameters=task_ids,
        )

        import_germline_snv_consequences_filter = RadiantStarRocksOperator(
            task_id="import_germline_snv_consequence_filter",
            sql="./sql/radiant/germline_snv_consequence_filter_insert.sql",
            task_display_name="[StarRocks] Insert Germline SNV Consequences Filter",
            submit_task_options=std_submit_task_opts,
        )

        insert_germline_snv_consequences_filter_part = RadiantStarRocksOperator(
            task_id="insert_germline_snv_consequence_filter_part",
            sql="./sql/radiant/germline_snv_consequence_filter_insert_part.sql",
            task_display_name="[StarRocks] Insert Germline SNV Consequences Filter Part",
            submit_task_options=std_submit_task_opts,
            parameters={"part": "{{ params.part }}"},
        )

        (
            import_germline_snv_consequences
            >> import_germline_snv_consequences_filter
            >> insert_germline_snv_consequences_filter_part
        )

    update_sequencing_experiments = EmptyOperator(
        task_id="update_sequencing_experiment", task_display_name="[TODO] Update Sequencing Experiments"
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
        >> insert_hashes
        >> overwrite_germline_snv_tmp_variants
        >> insert_exomiser
        >> insert_germline_snv_occurrences
        >> insert_stg_germline_snv_variants_freq
        >> aggregate_germline_snv_variants_frequencies
        >> tg_variants
        >> tg_consequences
        >> update_sequencing_experiments
    )


import_part()
