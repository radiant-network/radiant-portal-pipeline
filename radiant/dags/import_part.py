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

from radiant.dags import DEFAULT_ARGS, IS_AWS, NAMESPACE, ECSEnv, get_namespace, load_docs_md
from radiant.tasks.data.radiant_tables import get_iceberg_germline_snv_mapping
from radiant.tasks.starrocks.operator import (
    RadiantLoadExomiserOperator,
    RadiantStarRocksOperator,
    RadiantStarRocksPartitionSwapOperator,
    SubmitTaskOptions,
    SwapPartition,
)
from radiant.tasks.vcf.experiment import Case, Experiment

LOGGER = logging.getLogger(__name__)


PATH_TO_PYTHON_BINARY = os.getenv("RADIANT_PYTHON_PATH", "/home/airflow/.venv/radiant/bin/python")


def cases_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    column_names = [desc[0] for desc in descriptions[0]]
    dict_rows = [dict(zip(column_names, row, strict=False)) for row in results[0]]

    cases = []
    for case_id, grouped_rows in groupby(dict_rows, key=lambda x: x["case_id"]):
        list_rows = list(grouped_rows)

        # Force the proband vcf_filepath or if not available take the first one from the case
        _proband_files = [row["vcf_filepath"] for row in list_rows if row["family_role"] == "proband"]
        _vcf_filepath = _proband_files[0] if _proband_files else list_rows[0]["vcf_filepath"]

        case = Case(
            case_id=case_id,
            vcf_filepath=_vcf_filepath,
            part=list_rows[0]["part"],
            analysis_type=list_rows[0]["analysis_type"],
            experiments=[
                Experiment(
                    seq_id=row["seq_id"],
                    task_id=row["task_id"],
                    patient_id=row["patient_id"],
                    aliquot=row["aliquot"],
                    family_role=row["family_role"],
                    sex=row["sex"],
                    affected_status=row["affected_status"],
                    experimental_strategy=row["experimental_strategy"],
                    request_id=row["request_id"],
                    request_priority=row["request_priority"],
                    cnv_vcf_filepath=row["cnv_vcf_filepath"],
                    exomiser_filepath=row["exomiser_filepath"],
                )
                for row in list_rows
            ],
        )
        cases.append(case.model_dump())

    return [cases]


dag_params = {
    "part": Param(
        default=None,
        description="Integer that represents the part that need to be processed. ",
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
        output_processor=cases_output_processor,
        parameters={"part": "{{ params.part }}"},
    )

    @task.short_circuit(task_id="sanity_check_cases", task_display_name="[PyOp] Sanity Check Cases")
    def check_cases(cases: Any) -> Any:
        return cases

    @task(task_id="ecs_store_cases", task_display_name="[PyOp] ECS Store Cases")
    def ecs_store_cases(cases: Any) -> Any:
        from radiant.dags.operators.utils import s3_store_content

        # ECS limits the length of the command override, so we need to upload the cases to S3
        # and pass the S3 path of the file in which the data is stored to the ECS operator instead of the data.
        s3_path = s3_store_content(content=cases, ecs_env=ecs_env, prefix="commit_partitions")
        return [{"stored_cases": s3_path}]

    cases = check_cases(fetch_sequencing_experiment_delta.output)
    stored_cases = ecs_store_cases(cases) if IS_AWS else None

    @task(task_id="prepare_config", task_display_name="[PyOp] Prepare Config")
    def prepare_config(cases: Any) -> Any:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        conf = context["dag_run"].conf or {}
        conf["cases"] = cases
        return conf

    _prepare_config = prepare_config(cases)
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

        try:
            from radiant.dags.operators import ecs
        except ImportError as ie:
            LOGGER.error("ECS provider not found. Please install the required provider.")
            raise ie

        import_cnv_vcf = ecs.ImportPart.get_import_cnv_vcf(
            radiant_namespace=get_namespace(),
            ecs_env=ecs_env,
        )

    else:
        try:
            from radiant.dags.operators import k8s
        except ImportError as ie:
            LOGGER.error("Kubernetes provider not found. Please install the required provider.")
            raise ie

        import_cnv_vcf = k8s.ImportPart.get_import_cnv_vcf(get_namespace())

    @task(task_id="extract_seq_ids", task_display_name="[PyOp] Extract Sequencing Experiment IDs")
    def extract_sequencing_ids(cases) -> dict[str, list[Any]]:
        seq_ids = []
        for case in cases:
            for experiment in case.get("experiments"):
                if experiment:
                    seq_ids.append(experiment["seq_id"])
        return {"seq_ids": seq_ids}

    sequencing_ids = extract_sequencing_ids(cases)

    insert_germline_cnv_occurrences = RadiantStarRocksPartitionSwapOperator(
        task_id="insert_germline_cnv_occurrences",
        table="{{ mapping.starrocks_germline_cnv_occurrence }}",
        task_display_name="[StarRocks] Insert CNV Occurrences Part",
        # submit_task_options=SubmitTaskOptions(max_query_timeout=3600, poll_interval=10),
        swap_partition=SwapPartition(
            partition="{{ params.part }}",
            copy_partition_sql="./sql/radiant/germline_cnv_occurrence_copy_partition.sql",
        ),
        parameters=sequencing_ids,
        insert_partition_sql="./sql/radiant/germline_cnv_occurrence_insert_partition_delta.sql",
    )

    load_exomiser = RadiantLoadExomiserOperator(
        task_id="load_exomiser_files",
        task_display_name="[StarRocks] Load Exomiser Files",
        swap_partition=SwapPartition(
            partition="{{ params.part }}",
            copy_partition_sql="./sql/radiant/staging_exomiser_insert_partition_delta.sql",
        ),
        table="{{ mapping.starrocks_staging_exomiser }}",
        parameters=sequencing_ids,
        cases=cases,
    )

    @task(task_id="extract_case_ids", task_display_name="[PyOp] Extract Case IDs")
    def extract_case_ids(cases) -> dict[str, list[Any]]:
        return {"case_ids": [c["case_id"] for c in cases]}

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

    case_ids = extract_case_ids(cases)

    insert_hashes = RadiantStarRocksOperator(
        task_id="insert_variant_hashes",
        sql="./sql/radiant/variant_insert_hashes.sql",
        task_display_name="[StarRocks] Insert Variants Hashes into Lookup",
        submit_task_options=std_submit_task_opts,
        parameters=case_ids,
    )

    overwrite_tmp_variants = RadiantStarRocksOperator(
        task_id="overwrite_tmp_variant",
        sql="./sql/radiant/tmp_variant_insert.sql",
        task_display_name="[StarRocks] Insert Variants Tmp tables",
        submit_task_options=std_submit_task_opts,
        parameters=case_ids,
    )

    insert_exomiser = RadiantStarRocksOperator(
        task_id="insert_exomiser",
        sql="./sql/radiant/exomiser_insert.sql",
        task_display_name="[StarRocks] Insert Exomiser",
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
    )

    insert_occurrences = RadiantStarRocksOperator(
        task_id="insert_occurrence",
        sql="./sql/radiant/occurrence_insert.sql",
        task_display_name="[StarRocks] Insert Occurrences Part",
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
    )

    insert_stg_variants_freq = RadiantStarRocksOperator(
        task_id="insert_stg_variant_freq",
        sql="./sql/radiant/staging_variant_freq_insert.sql",
        task_display_name="[StarRocks] Insert Stg Variants Freq Part",
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
    )

    aggregate_variants_frequencies = RadiantStarRocksOperator(
        task_id="aggregate_variant_freq",
        task_display_name="[StarRocks] Aggregate all variants frequencies",
        sql="./sql/radiant/variant_frequency_insert.sql",
        submit_task_options=std_submit_task_opts,
    )

    with TaskGroup(group_id="variant") as tg_variants:
        insert_staging_variants = RadiantStarRocksOperator(
            task_id="insert_staging_variant",
            task_display_name="[StarRocks] Insert Staging Variants",
            sql="./sql/radiant/staging_variant_insert.sql",
            submit_task_options=std_submit_task_opts,
        )

        insert_variants_with_freqs = RadiantStarRocksOperator(
            task_id="insert_variant",
            task_display_name="[StarRocks] Insert Variants",
            sql="./sql/radiant/variant_insert.sql",
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

        insert_variants_part = RadiantStarRocksOperator(
            task_id="insert_variant_part",
            sql="./sql/radiant/variant_part_insert_part.sql",
            task_display_name="[StarRocks] Insert Variants Part",
            submit_task_options=std_submit_task_opts,
            parameters=_compute_part,
        )

        insert_staging_variants >> insert_variants_with_freqs >> insert_variants_part

    with TaskGroup(group_id="consequence") as tg_consequences:
        import_consequences = RadiantStarRocksOperator(
            task_id="import_consequence",
            sql="./sql/radiant/consequence_insert.sql",
            task_display_name="[StarRocks] Insert Consequences",
            submit_task_options=std_submit_task_opts,
            parameters=case_ids,
        )

        import_consequences_filter = RadiantStarRocksOperator(
            task_id="import_consequence_filter",
            sql="./sql/radiant/consequence_filter_insert.sql",
            task_display_name="[StarRocks] Insert Consequences Filter",
            submit_task_options=std_submit_task_opts,
        )

        insert_consequences_filter_part = RadiantStarRocksOperator(
            task_id="insert_consequence_filter_part",
            sql="./sql/radiant/consequence_filter_insert_part.sql",
            task_display_name="[StarRocks] Insert Consequences Filter Part",
            submit_task_options=std_submit_task_opts,
            parameters={"part": "{{ params.part }}"},
        )

        import_consequences >> import_consequences_filter >> insert_consequences_filter_part

    update_sequencing_experiments = EmptyOperator(
        task_id="update_sequencing_experiment", task_display_name="[TODO] Update Sequencing Experiments"
    )

    (
        start
        >> fetch_sequencing_experiment_delta
        >> (
            import_vcf,
            import_cnv_vcf.expand(params=stored_cases) if IS_AWS else import_cnv_vcf(cases=cases),
        )
        >> load_exomiser
        >> refresh_iceberg_tables
        >> insert_germline_cnv_occurrences
        >> insert_hashes
        >> overwrite_tmp_variants
        >> insert_exomiser
        >> insert_occurrences
        >> insert_stg_variants_freq
        >> aggregate_variants_frequencies
        >> tg_variants
        >> tg_consequences
        >> update_sequencing_experiments
    )


import_part()
