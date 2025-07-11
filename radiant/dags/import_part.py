import datetime
import logging
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

LOGGER = logging.getLogger(__name__)


def cases_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    import json

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
                    exomiser_filepaths=json.loads(row["exomiser_filepaths"])
                    if isinstance(row.get("exomiser_filepaths"), str)
                    else row.get("exomiser_filepaths", None),
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

    @task(task_id="load_exomiser_files", task_display_name="[PyOp] Load Exomiser Files")
    def load_exomiser_files(cases: object, part: object) -> None:
        import os
        import time
        import uuid

        import jinja2
        from airflow.hooks.base import BaseHook

        from radiant.dags import DAGS_DIR
        from radiant.tasks.data.radiant_tables import get_radiant_mapping

        _query_params = get_radiant_mapping() | {"broker_load_timeout": 7200}

        _check_part_exists_sql = """
        SHOW PARTITIONS FROM {{ params.starrocks_staging_exomiser }} WHERE PartitionName='p%(part)s'
        """
        _check_part_exists_sql = jinja2.Template(_check_part_exists_sql).render({"params": _query_params})

        clean_temp_part_part_sql = """
        ALTER TABLE {{ params.starrocks_staging_exomiser }} 
        DROP TEMPORARY PARTITION IF EXISTS tp%(part)s;
        """
        clean_temp_part_part_sql = jinja2.Template(clean_temp_part_part_sql).render({"params": _query_params})

        create_tmp_part_sql = """
        ALTER TABLE {{ params.starrocks_staging_exomiser }}
        ADD TEMPORARY PARTITION IF NOT EXISTS tp%(part)s VALUES IN ('%(part)s');
        """
        create_tmp_part_sql = jinja2.Template(create_tmp_part_sql).render({"params": _query_params})

        _path = os.path.join(DAGS_DIR.resolve(), "sql/radiant/staging_exomiser_insert_partition_delta.sql")
        with open(_path) as f_in:
            insert_part_delta_sql = jinja2.Template(f_in.read()).render({"params": _query_params})

        swap_partition_sql = """
        ALTER TABLE {{ params.starrocks_staging_exomiser }}
        REPLACE PARTITION (p%(part)s) WITH TEMPORARY PARTITION (tp%(part)s);            
        """
        swap_partition_sql = jinja2.Template(swap_partition_sql).render({"params": _query_params})

        _path = os.path.join(DAGS_DIR.resolve(), "sql/radiant/staging_exomiser_load.sql")
        with open(_path) as f_in:
            load_staging_exomiser_sql = jinja2.Template(f_in.read()).render({"params": _query_params})

        conn = BaseHook.get_connection("starrocks_conn")

        _parameters = []
        _seq_ids = []
        for case in cases:
            _case = Case.model_validate(case)
            for exp in _case.experiments:
                if not exp.exomiser_filepaths:
                    continue
                _parameters.append(
                    {
                        "part": _case.part,
                        "seq_id": exp.seq_id,
                        "tsv_filepaths": exp.exomiser_filepaths,
                        "label": f"load_exomiser_{_case.case_id}_{exp.seq_id}_{exp.task_id}_{str(uuid.uuid4().hex)}",
                    }
                )
                _seq_ids.append(exp.seq_id)

        if not _parameters:
            LOGGER.info("No Exomiser files to load, skipping...")
            return

        with conn.get_hook().get_conn().cursor() as cursor:
            # Execute the SQL to check if the partition exists
            LOGGER.info("Checking if partition exists...")
            cursor.execute(_check_part_exists_sql, {"part": part})
            _part_exists = bool(cursor.fetchone())
            LOGGER.info(f"Partition p{part} exists: {_part_exists}")

            if _part_exists:
                LOGGER.info(f"Cleaning temporary partition tp{part}...")
                cursor.execute(clean_temp_part_part_sql, {"part": part})
                LOGGER.info(f"Creating temporary partition tp{part}...")
                cursor.execute(create_tmp_part_sql, {"part": part})

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

            for _params in _parameters:
                LOGGER.info(f"Loading Exomiser file {_params['tsv_filepaths']}...")
                _paths = _params.pop("tsv_filepaths")
                for tsv_filepath in _paths:
                    _params["tsv_filepath"] = tsv_filepath
                    LOGGER.info(f"Executing exomiser load with params: {_params}")
                    _sql = load_staging_exomiser_sql.format(
                        label=f"{_params['label']}",
                        temporary_partition_clause="TEMPORARY PARTITION (tp%(part)s)" if _part_exists else "",
                        broker_configuration=broker_configuration,
                    )
                    cursor.execute(_sql, _params)

                _i = 0
                while True:
                    cursor.execute("SELECT STATE FROM information_schema.loads WHERE LABEL = %(label)s", _params)
                    load_state = cursor.fetchone()
                    LOGGER.info(f"Load state for label {_params['label']}: {load_state}")
                    if not load_state or load_state[0] == "FINISHED":
                        break
                    if load_state[0] == "CANCELLED":
                        raise RuntimeError(f"Load for label {_params['label']} was cancelled.")
                    time.sleep(2)
                    _i += 1
                    if _i > 30:
                        raise TimeoutError(f"Load for label {_params['label']} did not finish in time.")

            if _part_exists:
                LOGGER.info(f"Inserting partition delta for part {part}...")
                cursor.execute(insert_part_delta_sql, {"seq_ids": _seq_ids, "part": part})
                LOGGER.info(f"Swapping temporary partition tp{part} with partition p{part}...")
                cursor.execute(swap_partition_sql, {"part": part})

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
        >> import_vcf
        >> refresh_iceberg_tables
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
