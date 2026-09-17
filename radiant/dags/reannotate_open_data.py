import datetime
import logging
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from radiant.dags import DEFAULT_ARGS, NAMESPACE, RADIANT_LOCK_S3_BUCKET, load_docs_md
from radiant.tasks.locking import IMPORT_MUTEX_LOCK_NAME, acquire_lock, release_lock
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions

LOGGER = logging.getLogger(__name__)

std_submit_task_opts = SubmitTaskOptions(max_query_timeout=14400, poll_interval=30)

PARTS_PER_VARIANT_PART = 10


def build_variant_part_params(tenant_parts: list[dict]) -> list[dict[str, Any]]:
    pairs = sorted({(row["tenant_code"], int(row["part"]) // PARTS_PER_VARIANT_PART) for row in tenant_parts})
    return [
        {
            "tenant_code": tenant_code,
            "parameters": {
                "variant_part": variant_part,
                "part_lower": variant_part * PARTS_PER_VARIANT_PART,
                "part_upper": (variant_part + 1) * PARTS_PER_VARIANT_PART,
            },
        }
        for tenant_code, variant_part in pairs
    ]


def build_cnv_params(tenant_parts: list[dict]) -> list[dict[str, Any]]:
    return [
        {"tenant_code": row["tenant_code"], "parameters": {"part": int(row["part"])}}
        for row in sorted(tenant_parts, key=lambda r: (r["tenant_code"], int(r["part"])))
    ]


@dag(
    start_date=datetime.datetime(2021, 1, 1),
    # Manual until an operator flips this to the weekly Saturday 00:00 run (§4); see the module docstring.
    #   schedule="0 0 * * 6",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["radiant", "starrocks", "open-data", "manual"],
    dag_display_name="Radiant - Re-annotate against OpenDataLake",
    dag_id=f"{NAMESPACE}-reannotate-open-data",
    render_template_as_native_obj=True,
    doc_md=load_docs_md("reannotate_open_data.md"),
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def reannotate_open_data():
    @task(
        task_id="acquire_import_lock",
        task_display_name="[PyOp] Acquire Import Lock",
    )
    def acquire_import_lock():
        from airflow.operators.python import get_current_context

        context = get_current_context()
        holder = f"{context['dag'].dag_id}:{context['run_id']}"
        acquire_lock(bucket=RADIANT_LOCK_S3_BUCKET, name=IMPORT_MUTEX_LOCK_NAME, holder=holder)

    @task(task_id="release_import_lock", task_display_name="[PyOp] Release Import Lock")
    def release_import_lock():
        release_lock(bucket=RADIANT_LOCK_S3_BUCKET, name=IMPORT_MUTEX_LOCK_NAME)

    _acquire_import_lock = acquire_import_lock()
    _release_import_lock = release_import_lock()

    @task(task_id="ensure_tables_exist", task_display_name="[PyOp] Preflight: Tables Exist?")
    def ensure_tables_exist():
        from airflow.exceptions import AirflowFailException
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.open_data import format_missing_tables, list_missing_open_data_tables

        conf = get_current_context()["dag_run"].conf or {}
        missing = list_missing_open_data_tables(conf)
        if missing:
            raise AirflowFailException(format_missing_tables(missing))

    _ensure_tables_exist = ensure_tables_exist()

    reference_load = TriggerDagRunOperator(
        task_id="reference_load",
        task_display_name="[DAG] Refresh Open Data from OpenDataLake",
        trigger_dag_id=f"{NAMESPACE}-import-open-data",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    sources_loaded = EmptyOperator(
        task_id="sources_loaded",
        task_display_name="[ --- CHECKPOINT: PHASE 2 --- ] All New Sources Are Loaded",
    )

    @task.short_circuit(
        task_id="extract_all_tenants",
        task_display_name="[PyOp] Extract All Tenants",
        ignore_downstream_trigger_rules=False,
    )
    def extract_all_tenants() -> list[str]:
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.tenants import list_all_tenants

        context = get_current_context()
        return list_all_tenants(context["dag_run"].conf or {})

    @task.short_circuit(
        task_id="extract_all_parts",
        task_display_name="[PyOp] Extract All Parts",
        ignore_downstream_trigger_rules=False,
    )
    def extract_all_parts() -> list[int]:
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.tenants import list_all_parts

        context = get_current_context()
        return list_all_parts(context["dag_run"].conf or {})

    @task.short_circuit(
        task_id="extract_tenant_parts",
        task_display_name="[PyOp] Extract Tenant/Part Pairs",
        ignore_downstream_trigger_rules=False,
    )
    def extract_tenant_parts() -> list[dict]:
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.tenants import list_tenant_parts

        context = get_current_context()
        return list_tenant_parts(context["dag_run"].conf or {})

    all_tenants = extract_all_tenants()
    all_parts = extract_all_parts()
    tenant_parts = extract_tenant_parts()

    @task(task_id="build_variant_part_params", task_display_name="[PyOp] Per-tenant Variant-Part Params")
    def variant_part_params(tenant_parts: list[dict]) -> list[dict[str, Any]]:
        return build_variant_part_params(tenant_parts)

    @task(task_id="build_cnv_params", task_display_name="[PyOp] Per-tenant CNV Part Params")
    def cnv_params(tenant_parts: list[dict]) -> list[dict[str, Any]]:
        return build_cnv_params(tenant_parts)

    @task(task_id="build_part_params", task_display_name="[PyOp] Per-part Params")
    def part_params(parts: list[int]) -> list[dict[str, Any]]:
        return [{"part": part} for part in parts]

    _variant_part_params = variant_part_params(tenant_parts)
    _cnv_params = cnv_params(tenant_parts)
    _part_params = part_params(all_parts)

    @task(task_id="render_pooled_sql", task_display_name="[PyOp] Render Pooled SQL")
    def render_pooled_sql(sql_file: str, tenants: list[str]) -> str:
        import jinja2
        from airflow.operators.python import get_current_context

        from radiant.dags import DAGS_DIR
        from radiant.tasks.data.radiant_tables import get_radiant_mapping

        conf = get_current_context()["dag_run"].conf or {}
        text = (DAGS_DIR / "sql" / sql_file).read_text()
        return jinja2.Template(text).render(
            mapping=get_radiant_mapping(conf),
            tenants=tenants,
            per_tenant_mapping=lambda t: get_radiant_mapping(conf, tenant_code=t),
        )

    with TaskGroup(group_id="reannotate_accumulators") as tg_accumulators:
        reannotate_staging_variant = RadiantStarRocksOperator(
            task_id="reannotate_snv_staging_variant",
            task_display_name="[StarRocks] Re-annotate Staging SNV Variants",
            sql="./sql/radiant/snv_staging_variant_reannotate.sql",
            submit_task_options=std_submit_task_opts,
        )

        reannotate_consequence = RadiantStarRocksOperator(
            task_id="reannotate_snv_consequence",
            task_display_name="[StarRocks] Re-annotate SNV Consequences",
            sql="./sql/radiant/snv_consequence_reannotate.sql",
            submit_task_options=std_submit_task_opts,
        )

    with TaskGroup(group_id="snv_variant") as tg_variants:
        insert_snv_variants = RadiantStarRocksOperator.partial(
            task_id="insert_snv_variant",
            task_display_name="[StarRocks] Insert SNV Variants",
            sql="./sql/radiant/snv_variant_insert.sql",
            map_index_template="{{ task.tenant_code }}",
            submit_task_options=std_submit_task_opts,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            max_active_tis_per_dagrun=1,
        ).expand(tenant_code=all_tenants)

        insert_snv_variants_part = RadiantStarRocksOperator.partial(
            task_id="insert_snv_variant_part",
            task_display_name="[StarRocks] Insert SNV Variants Part",
            sql="./sql/radiant/snv_variant_part_insert_part.sql",
            map_index_template="{{ task.tenant_code }}",
            submit_task_options=std_submit_task_opts,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            max_active_tis_per_dagrun=1,
        ).expand_kwargs(_variant_part_params)

        insert_snv_variants >> insert_snv_variants_part

    with TaskGroup(group_id="snv_consequence") as tg_consequences:
        cons_filter_sql = render_pooled_sql.override(
            task_id="render_snv_consequence_filter_part_sql",
            task_display_name="[PyOp] Render Pooled SNV Consequence Filter Part SQL",
        )("radiant/snv_consequence_filter_insert_part.sql", all_tenants)

        insert_consequence_filter = RadiantStarRocksOperator(
            task_id="insert_snv_consequence_filter",
            task_display_name="[StarRocks] Insert SNV Consequences Filter",
            sql="./sql/radiant/snv_consequence_filter_insert.sql",
            submit_task_options=std_submit_task_opts,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        insert_consequence_filter_part = RadiantStarRocksOperator.partial(
            task_id="insert_snv_consequence_filter_part",
            task_display_name="[StarRocks] Insert SNV Consequences Filter Part",
            sql=cons_filter_sql,
            submit_task_options=std_submit_task_opts,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            max_active_tis_per_dagrun=1,
        ).expand(parameters=_part_params)

        insert_consequence_filter >> insert_consequence_filter_part

    with TaskGroup(group_id="cnv_occurrence") as tg_cnv:
        reannotate_germline_cnv = RadiantStarRocksOperator.partial(
            task_id="reannotate_germline_cnv_occurrence",
            task_display_name="[StarRocks] Re-annotate Germline CNV Occurrences",
            sql="./sql/radiant/germline_cnv_occurrence_reannotate_partition.sql",
            map_index_template="{{ task.tenant_code }}",
            submit_task_options=std_submit_task_opts,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            max_active_tis_per_dagrun=1,
        ).expand_kwargs(_cnv_params)

        reannotate_somatic_cnv = RadiantStarRocksOperator.partial(
            task_id="reannotate_somatic_cnv_occurrence",
            task_display_name="[StarRocks] Re-annotate Somatic CNV Occurrences",
            sql="./sql/radiant/somatic_cnv_occurrence_reannotate_partition.sql",
            map_index_template="{{ task.tenant_code }}",
            submit_task_options=std_submit_task_opts,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            max_active_tis_per_dagrun=1,
        ).expand_kwargs(_cnv_params)

        reannotate_germline_cnv >> reannotate_somatic_cnv

    @task(task_id="build_release_rows", task_display_name="[PyOp] Build Release Rows")
    def build_release_rows() -> list[dict[str, str]]:
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.radiant_tables import (
            ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
            RadiantConfigKeys,
            get_config_value,
        )

        conf = get_current_context()["dag_run"].conf or {}
        catalog = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_CATALOG)
        database = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_DATABASE)
        ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)

        dataset_version = "" if ref in ("", "latest") else ref
        return [
            {
                "catalog_name": catalog,
                "database_name": database,
                "table_name": table,
                "iceberg_ref": ref,
                "dataset_version": dataset_version,
            }
            for table in sorted(ICEBERG_OPEN_DATA_CONTRACT_MAPPING.values())
        ]

    @task(task_id="render_release_sql", task_display_name="[PyOp] Render Release SQL")
    def render_release_sql(releases: list[dict[str, str]]) -> str:
        import jinja2
        from airflow.operators.python import get_current_context

        from radiant.dags import DAGS_DIR
        from radiant.tasks.data.radiant_tables import get_radiant_mapping

        context = get_current_context()
        conf = context["dag_run"].conf or {}
        text = (DAGS_DIR / "sql" / "radiant" / "open_data_release_insert.sql").read_text()
        return jinja2.Template(text).render(
            mapping=get_radiant_mapping(conf),
            releases=releases,
            recorded_at=datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S"),
            dag_run_id=context["run_id"],
        )

    _release_rows = build_release_rows()
    _release_sql = render_release_sql(_release_rows)

    rebuilds_complete = EmptyOperator(
        task_id="rebuilds_complete",
        task_display_name="[ --- CHECKPOINT: FINAL PHASE --- ] Before Recording the Release",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    record_release = RadiantStarRocksOperator(
        task_id="record_open_data_release",
        task_display_name="[StarRocks] Record the Release",
        sql=_release_sql,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # --- Flow --------------------------------------------------------------------------------------
    _ensure_tables_exist >> _acquire_import_lock >> reference_load >> sources_loaded

    sources_loaded >> [all_tenants, all_parts, tenant_parts]
    sources_loaded >> tg_accumulators
    sources_loaded >> _release_rows

    reannotate_staging_variant >> insert_snv_variants
    reannotate_consequence >> insert_consequence_filter

    sources_loaded >> [tg_variants, tg_consequences]

    insert_snv_variants >> tg_cnv

    [tg_variants, tg_consequences, tg_cnv] >> rebuilds_complete >> record_release
    record_release >> _release_import_lock


reannotate_open_data()
