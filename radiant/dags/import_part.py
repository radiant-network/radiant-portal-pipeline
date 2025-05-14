import datetime
from collections.abc import Sequence
from itertools import groupby
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md
from radiant.tasks.data.radiant_tables import get_iceberg_germline_snv_mapping
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions
from radiant.tasks.vcf.experiment import Case, Experiment


def cases_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    column_names = [desc[0] for desc in descriptions[0]]
    dict_rows = [dict(zip(column_names, row, strict=False)) for row in results[0]]

    cases = []
    for case_id, grouped_rows in groupby(dict_rows, key=lambda x: x["case_id"]):
        list_rows = list(grouped_rows)
        case = Case(
            case_id=case_id,
            vcf_filepath=list_rows[0]["vcf_filepath"],
            part=list_rows[0]["part"],
            analysis_type=list_rows[0]["analysis_type"],
            experiments=[
                Experiment(
                    seq_id=row["seq_id"],
                    patient_id=row["patient_id"],
                    sample_id=row["sample_id"],
                    family_role=row["family_role"],
                    sex=row["sex"],
                    is_affected=row["is_affected"],
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
        sql="./sql/radiant/sequencing_experiment_select_delta.sql",
        task_display_name="[StarRocks] Get Sequencing Experiments",
        do_xcom_push=True,
        output_processor=cases_output_processor,
        parameters={"part": "{{ params.part }}"},
    )

    @task.short_circuit(task_id="sanity_check_cases", task_display_name="[PyOp] Sanity Check Cases")
    def check_cases(cases: Any) -> Any:
        return cases

    cases = check_cases(fetch_sequencing_experiment_delta.output)

    import_vcf = TriggerDagRunOperator(
        task_id="import_vcf",
        task_display_name="[DAG] Import VCF into Iceberg",
        trigger_dag_id=f"{NAMESPACE}-import-vcf",
        conf={"cases": cases},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=2,
    )

    @task(task_id="extract_case_ids", task_display_name="[PyOp] Extract Case IDs")
    def extract_case_ids(cases) -> dict[str, list[Any]]:
        return {"case_ids": [c["case_id"] for c in cases]}

    refresh_iceberg_tables = RadiantStarRocksOperator.partial(
        task_id="refresh_iceberg_tables",
        sql="REFRESH EXTERNAL TABLE {{ params.table }}",
        task_display_name="[StarRocks] Refresh Iceberg Tables",
    ).expand(params=[{"table": v} for v in get_iceberg_germline_snv_mapping().values()])

    case_ids = extract_case_ids(cases)

    insert_hashes = RadiantStarRocksOperator(
        task_id="insert_variants_hashes",
        sql="./sql/radiant/variants_insert_hashes.sql",
        task_display_name="[StarRocks] Insert Variants Hashes into Lookup",
        submit_task_options=std_submit_task_opts,
        parameters=case_ids,
    )

    overwrite_tmp_variants = RadiantStarRocksOperator(
        task_id="overwrite_tmp_variants",
        sql="./sql/radiant/tmp_variants_insert.sql",
        task_display_name="[StarRocks] Insert Variants Tmp tables",
        submit_task_options=std_submit_task_opts,
        parameters=case_ids,
    )

    insert_occurrences = RadiantStarRocksOperator(
        task_id="insert_occurrences",
        sql="./sql/radiant/occurrences_insert.sql",
        task_display_name="[StarRocks] Insert Occurrences Part",
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
    )

    insert_stg_variants_freq = RadiantStarRocksOperator(
        task_id="insert_stg_variants_freq",
        sql="./sql/radiant/staging_variants_freq_insert.sql",
        task_display_name="[StarRocks] Insert Stg Variants Freq Part",
        submit_task_options=std_submit_task_opts,
        parameters={"part": "{{ params.part }}"},
    )

    aggregate_variants_frequencies = RadiantStarRocksOperator(
        task_id="aggregate_variants_freq",
        task_display_name="[StarRocks] Aggregate all variants frequencies",
        sql="./sql/radiant/variants_frequencies_insert.sql",
        submit_task_options=std_submit_task_opts,
    )

    with TaskGroup(group_id="variants") as tg_variants:
        insert_staging_variants = RadiantStarRocksOperator(
            task_id="insert_staging_variants",
            task_display_name="[StarRocks] Insert Staging Variants",
            sql="./sql/radiant/staging_variants_insert.sql",
            submit_task_options=std_submit_task_opts,
        )

        insert_variants_with_freqs = RadiantStarRocksOperator(
            task_id="insert_variants",
            task_display_name="[StarRocks] Insert Variants",
            sql="./sql/radiant/variants_insert.sql",
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
            task_id="insert_variants_part",
            sql="./sql/radiant/variants_part_insert_part.sql",
            task_display_name="[StarRocks] Insert Variants Part",
            submit_task_options=std_submit_task_opts,
            parameters=_compute_part,
        )

        insert_staging_variants >> insert_variants_with_freqs >> insert_variants_part

    with TaskGroup(group_id="consequences") as tg_consequences:
        import_consequences = RadiantStarRocksOperator(
            task_id="import_consequences",
            sql="./sql/radiant/consequences_insert.sql",
            task_display_name="[StarRocks] Insert Consequences",
            submit_task_options=std_submit_task_opts,
            parameters=case_ids,
        )

        import_consequences_filter = RadiantStarRocksOperator(
            task_id="import_consequences_filter",
            sql="./sql/radiant/consequences_filter_insert.sql",
            task_display_name="[StarRocks] Insert Consequences Filter",
            submit_task_options=std_submit_task_opts,
        )

        insert_consequences_filter_part = RadiantStarRocksOperator(
            task_id="insert_consequences_filter_part",
            sql="./sql/radiant/consequences_filter_insert_part.sql",
            task_display_name="[StarRocks] Insert Consequences Filter Part",
            submit_task_options=std_submit_task_opts,
            parameters={"part": "{{ params.part }}"},
        )

        import_consequences >> import_consequences_filter >> insert_consequences_filter_part

    update_sequencing_experiments = EmptyOperator(
        task_id="update_sequencing_experiments", task_display_name="[TODO] Update Sequencing Experiments"
    )

    (
        start
        >> fetch_sequencing_experiment_delta
        >> import_vcf
        >> refresh_iceberg_tables
        >> insert_hashes
        >> overwrite_tmp_variants
        >> insert_occurrences
        >> insert_stg_variants_freq
        >> aggregate_variants_frequencies
        >> tg_variants
        >> tg_consequences
        >> update_sequencing_experiments
    )


import_part()
