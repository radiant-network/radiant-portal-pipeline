"""Weekly OpenDataLake refresh and re-annotation (SJRA-1811 §4, §5).

P1 -> checkpoint -> P2 -> P3a / P3b -> P4, exactly the flow in `design/SJRA-1811-opendatalake-integration.md`.
The whole run holds the `import_mutex` S3 lock, which is the reason this is one DAG rather than several:
a pool releases its slot when a *task* ends, so only a lock held across the run can exclude `import_part`.

=======================================================================================================
STAGED ROLLOUT -- only P1 is live. Everything below it is commented out, not deleted.
=======================================================================================================

LOCK DISABLED as well, for the sandbox. `acquire_lock` uses `PutObject` with `If-None-Match: *`, and
MinIO supports conditional writes only against an exact ETag, never the `*` wildcard -- minio#20346 is
closed as "working as intended", and `minio/minio` was archived in April 2026, so no image bump fixes
it. Nothing is lost meanwhile: the mutex exists to keep this DAG and `import_part` apart, and this DAG
is not live yet. Restore the two tasks, their imports and their edges before it is.

Current graph:

    preflight_tables_exist -> reference_load -> sources_loaded

To bring a phase online, uncomment its block **and** its edge in the Flow section at the bottom, then
re-point the lock release. Suggested order, each one runnable on its own:

    1. P1   reference_load                     <-- live now
    2. P2   reannotate_accumulators            + the three discovery tasks it does not need (none)
    3. P3a  snv_variant / snv_consequence      + `extract_all_tenants`, `extract_all_parts`,
                                                 `extract_tenant_parts`, the `build_*_params` tasks
                                                 and `render_pooled_sql`
    4. P3b  cnv_occurrence                     + `extract_tenant_parts` / `build_cnv_params`
    5. P4   record_open_data_release

Two things to move each time:
  * the lock release currently hangs off `sources_loaded`; it must always hang off the *last* live task,
    so that a failure anywhere leaves the mutex held (§4).
  * the imports at the top -- `TaskGroup`, `TriggerRule`, `RadiantStarRocksOperator`, `SubmitTaskOptions`
    and `std_submit_task_opts` -- are commented out with the phases that use them.

Tests covering the disabled phases are marked skipped in `tests/unit/dags/test_reannotate_open_data.py`
with the same `SJRA-1811 staged rollout` reason; grep for it to find everything that has to come back.
"""

import datetime
import logging
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Re-enable with P2 onwards.
# from airflow.utils.task_group import TaskGroup
# from airflow.utils.trigger_rule import TriggerRule
from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md

# Re-enable with the lock tasks. See the LOCK DISABLED note in the module docstring.
# from radiant.dags import RADIANT_LOCK_S3_BUCKET
# from radiant.tasks.locking import IMPORT_MUTEX_LOCK_NAME, acquire_lock, release_lock

# Re-enable with P2 onwards.
# from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, SubmitTaskOptions

LOGGER = logging.getLogger(__name__)

# A re-annotation scans whole tables, not a batch, so every statement here is long-running by
# construction. Same shape as `import_part`'s options, with a longer ceiling.
# Re-enable with P2 onwards.
# std_submit_task_opts = SubmitTaskOptions(max_query_timeout=14400, poll_interval=30)

# A variant-part is 10 occurrence-parts (SJRA-1811 §5). Same constant as `import_part.compute_part`.
PARTS_PER_VARIANT_PART = 10


# The two builders below stay live and unit-tested even while their tasks are commented out: they are
# pure functions, so they cost nothing here and keep their coverage while the phases are staged in.
def build_variant_part_params(tenant_parts: list[dict]) -> list[dict[str, Any]]:
    """(tenant, variant-part) pairs, deduplicated, from the (tenant, part) pairs that exist.

    Ten occurrence-parts collapse into one variant-part, so the fan-out is parts/10 per tenant --
    which is the cardinality §5 calls out as the real one for `snv__variant_partitioned`.
    """
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
    """One mapped task per (tenant, part) that exists -- not per cell of the cross product."""
    return [
        {"tenant_code": row["tenant_code"], "parameters": {"part": int(row["part"])}}
        for row in sorted(tenant_parts, key=lambda r: (r["tenant_code"], int(r["part"])))
    ]


@dag(
    start_date=datetime.datetime(2021, 1, 1),
    # STAGED ROLLOUT: manual trigger only. A partial DAG must not fire on its own and take the mutex.
    # Restore the weekly schedule -- Saturday 00:00 (SJRA-1811 §4) -- when P4 goes live:
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
    # LOCK DISABLED -- see the module docstring. Restore both tasks, their imports, and the two edges
    # in the Flow section together.
    # @task(
    #     task_id="acquire_import_lock",
    #     task_display_name="[PyOp] Acquire Import Lock",
    # )
    # def acquire_import_lock():
    #     from airflow.operators.python import get_current_context
    #
    #     context = get_current_context()
    #     holder = f"{context['dag'].dag_id}:{context['run_id']}"
    #     acquire_lock(bucket=RADIANT_LOCK_S3_BUCKET, name=IMPORT_MUTEX_LOCK_NAME, holder=holder)
    #
    # @task(task_id="release_import_lock", task_display_name="[PyOp] Release Import Lock")
    # def release_import_lock():
    #     release_lock(bucket=RADIANT_LOCK_S3_BUCKET, name=IMPORT_MUTEX_LOCK_NAME)
    #
    # _acquire_import_lock = acquire_import_lock()
    # _release_import_lock = release_import_lock()

    # --- Phase 0: preflight -----------------------------------------------------------------------
    # Ahead of the lock, deliberately. A missing table is a setup problem, not a race, so failing here
    # costs nothing and leaves no mutex to clear -- which matters while the DAG is being brought up one
    # phase at a time and this failure is an expected outcome.
    @task(task_id="preflight_tables_exist", task_display_name="[PyOp] P0 - Preflight: Tables Exist?")
    def preflight_tables_exist():
        from airflow.exceptions import AirflowFailException
        from airflow.operators.python import get_current_context

        from radiant.tasks.data.open_data import format_missing_tables, list_missing_open_data_tables

        conf = get_current_context()["dag_run"].conf or {}
        missing = list_missing_open_data_tables(conf)
        if missing:
            # AirflowFailException, not a bare raise: no retry can conjure a table into existence, and
            # the reference load reports one gap per run, so the whole list in one message is the point.
            raise AirflowFailException(format_missing_tables(missing))

    _preflight_tables_exist = preflight_tables_exist()

    # --- Phase 1: reference load ------------------------------------------------------------------
    # Triggered rather than reimplemented: `import-open-data` already is the reference load, and it stays
    # independently runnable for an operator who wants one source refreshed without a re-annotation.
    # It is triggered with no conf, so its two file-driven broker loads (RCV summary, cytoband) skip.
    reference_load = TriggerDagRunOperator(
        task_id="reference_load",
        task_display_name="[DAG] P1 - Refresh Open Data from OpenDataLake",
        trigger_dag_id=f"{NAMESPACE}-import-open-data",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
    )

    sources_loaded = EmptyOperator(
        task_id="sources_loaded",
        task_display_name="[ --- CHECKPOINT --- ] All New Sources Are Loaded",
    )

    # =================================================================================================
    # STAGED ROLLOUT: everything from here to the Flow section is disabled. See the module docstring.
    # =================================================================================================

    # --- Discovery: tenants and parts (needed by P3a and P3b) --------------------------------------
    # @task.short_circuit(task_id="extract_all_tenants", task_display_name="[PyOp] Extract All Tenants")
    # def extract_all_tenants() -> list[str]:
    #     from airflow.operators.python import get_current_context
    #
    #     from radiant.tasks.data.tenants import list_all_tenants
    #
    #     context = get_current_context()
    #     return list_all_tenants(context["dag_run"].conf or {})
    #
    # @task.short_circuit(task_id="extract_all_parts", task_display_name="[PyOp] Extract All Parts")
    # def extract_all_parts() -> list[int]:
    #     from airflow.operators.python import get_current_context
    #
    #     from radiant.tasks.data.tenants import list_all_parts
    #
    #     context = get_current_context()
    #     return list_all_parts(context["dag_run"].conf or {})
    #
    # @task.short_circuit(task_id="extract_tenant_parts", task_display_name="[PyOp] Extract Tenant/Part Pairs")
    # def extract_tenant_parts() -> list[dict]:
    #     from airflow.operators.python import get_current_context
    #
    #     from radiant.tasks.data.tenants import list_tenant_parts
    #
    #     context = get_current_context()
    #     return list_tenant_parts(context["dag_run"].conf or {})
    #
    # all_tenants = extract_all_tenants()
    # all_parts = extract_all_parts()
    # tenant_parts = extract_tenant_parts()
    #
    # @task(task_id="build_variant_part_params", task_display_name="[PyOp] Per-tenant Variant-Part Params")
    # def variant_part_params(tenant_parts: list[dict]) -> list[dict[str, Any]]:
    #     return build_variant_part_params(tenant_parts)
    #
    # @task(task_id="build_cnv_params", task_display_name="[PyOp] Per-tenant CNV Part Params")
    # def cnv_params(tenant_parts: list[dict]) -> list[dict[str, Any]]:
    #     return build_cnv_params(tenant_parts)
    #
    # @task(task_id="build_part_params", task_display_name="[PyOp] Per-part Params")
    # def part_params(parts: list[int]) -> list[dict[str, Any]]:
    #     return [{"part": part} for part in parts]
    #
    # _variant_part_params = variant_part_params(tenant_parts)
    # _cnv_params = cnv_params(tenant_parts)
    # _part_params = part_params(all_parts)
    #
    # @task(task_id="render_pooled_sql", task_display_name="[PyOp] Render Pooled SQL")
    # def render_pooled_sql(sql_file: str, tenants: list[str]) -> str:
    #     # Same helper as `import_part`: the consequence-filter part statement unions every tenant's
    #     # occurrence tables, which Jinja has to expand before StarRocks sees it.
    #     import jinja2
    #     from airflow.operators.python import get_current_context
    #
    #     from radiant.dags import DAGS_DIR
    #     from radiant.tasks.data.radiant_tables import get_radiant_mapping
    #
    #     conf = get_current_context()["dag_run"].conf or {}
    #     text = (DAGS_DIR / "sql" / sql_file).read_text()
    #     return jinja2.Template(text).render(
    #         mapping=get_radiant_mapping(conf),
    #         tenants=tenants,
    #         per_tenant_mapping=lambda t: get_radiant_mapping(conf, tenant_code=t),
    #     )

    # --- Phase 2: accumulators, upsert in place (Decision 4 Option A) ------------------------------
    # with TaskGroup(group_id="reannotate_accumulators") as tg_accumulators:
    #     reannotate_staging_variant = RadiantStarRocksOperator(
    #         task_id="reannotate_snv_staging_variant",
    #         task_display_name="[StarRocks] P2 - Re-annotate Staging SNV Variants",
    #         sql="./sql/radiant/snv_staging_variant_reannotate.sql",
    #         submit_task_options=std_submit_task_opts,
    #     )
    #
    #     reannotate_consequence = RadiantStarRocksOperator(
    #         task_id="reannotate_snv_consequence",
    #         task_display_name="[StarRocks] P2 - Re-annotate SNV Consequences",
    #         sql="./sql/radiant/snv_consequence_reannotate.sql",
    #         submit_task_options=std_submit_task_opts,
    #     )

    # --- Phase 3a: portal-facing SNV tables --------------------------------------------------------
    # Two independent chains; left-to-right inside a chain is not independent, because each partitioned
    # table is a partitioned copy of the unpartitioned one above it (§5).
    # with TaskGroup(group_id="snv_variant") as tg_variants:
    #     insert_snv_variants = RadiantStarRocksOperator.partial(
    #         task_id="insert_snv_variant",
    #         task_display_name="[StarRocks] P3a - Insert SNV Variants",
    #         sql="./sql/radiant/snv_variant_insert.sql",
    #         map_index_template="{{ task.tenant_code }}",
    #         submit_task_options=std_submit_task_opts,
    #         trigger_rule=TriggerRule.ALL_SUCCESS,
    #         max_active_tis_per_dagrun=1,
    #     ).expand(tenant_code=all_tenants)
    #
    #     insert_snv_variants_part = RadiantStarRocksOperator.partial(
    #         task_id="insert_snv_variant_part",
    #         task_display_name="[StarRocks] P3a - Insert SNV Variants Part",
    #         sql="./sql/radiant/snv_variant_part_insert_part.sql",
    #         map_index_template="{{ task.tenant_code }}",
    #         submit_task_options=std_submit_task_opts,
    #         trigger_rule=TriggerRule.ALL_SUCCESS,
    #         max_active_tis_per_dagrun=1,
    #     ).expand_kwargs(_variant_part_params)
    #
    #     insert_snv_variants >> insert_snv_variants_part
    #
    # with TaskGroup(group_id="snv_consequence") as tg_consequences:
    #     cons_filter_sql = render_pooled_sql.override(
    #         task_id="render_snv_consequence_filter_part_sql",
    #         task_display_name="[PyOp] Render Pooled SNV Consequence Filter Part SQL",
    #     )("radiant/snv_consequence_filter_insert_part.sql", all_tenants)
    #
    #     insert_consequence_filter = RadiantStarRocksOperator(
    #         task_id="insert_snv_consequence_filter",
    #         task_display_name="[StarRocks] P3a - Insert SNV Consequences Filter",
    #         sql="./sql/radiant/snv_consequence_filter_insert.sql",
    #         submit_task_options=std_submit_task_opts,
    #         trigger_rule=TriggerRule.ALL_SUCCESS,
    #     )
    #
    #     insert_consequence_filter_part = RadiantStarRocksOperator.partial(
    #         task_id="insert_snv_consequence_filter_part",
    #         task_display_name="[StarRocks] P3a - Insert SNV Consequences Filter Part",
    #         sql=cons_filter_sql,
    #         submit_task_options=std_submit_task_opts,
    #         trigger_rule=TriggerRule.ALL_SUCCESS,
    #         max_active_tis_per_dagrun=1,
    #     ).expand(parameters=_part_params)
    #
    #     insert_consequence_filter >> insert_consequence_filter_part

    # --- Phase 3b: CNV occurrences -----------------------------------------------------------------
    # Parallel to 3a on purpose: `locus_id`, `chromosome` and `start` are carried through re-annotation
    # unchanged, so the CNV rebuild does not care whether the SNV rebuild has run (§5).
    # with TaskGroup(group_id="cnv_occurrence") as tg_cnv:
    #     reannotate_germline_cnv = RadiantStarRocksOperator.partial(
    #         task_id="reannotate_germline_cnv_occurrence",
    #         task_display_name="[StarRocks] P3b - Re-annotate Germline CNV Occurrences",
    #         sql="./sql/radiant/germline_cnv_occurrence_reannotate_partition.sql",
    #         map_index_template="{{ task.tenant_code }}",
    #         submit_task_options=std_submit_task_opts,
    #         trigger_rule=TriggerRule.ALL_SUCCESS,
    #         max_active_tis_per_dagrun=1,
    #     ).expand_kwargs(_cnv_params)
    #
    #     reannotate_somatic_cnv = RadiantStarRocksOperator.partial(
    #         task_id="reannotate_somatic_cnv_occurrence",
    #         task_display_name="[StarRocks] P3b - Re-annotate Somatic CNV Occurrences",
    #         sql="./sql/radiant/somatic_cnv_occurrence_reannotate_partition.sql",
    #         map_index_template="{{ task.tenant_code }}",
    #         submit_task_options=std_submit_task_opts,
    #         trigger_rule=TriggerRule.ALL_SUCCESS,
    #         max_active_tis_per_dagrun=1,
    #     ).expand_kwargs(_cnv_params)
    #
    #     reannotate_germline_cnv >> reannotate_somatic_cnv

    # --- Phase 4: record the release ---------------------------------------------------------------
    # @task(task_id="build_release_rows", task_display_name="[PyOp] Build Release Rows")
    # def build_release_rows() -> list[dict[str, str]]:
    #     from airflow.operators.python import get_current_context
    #
    #     from radiant.tasks.data.radiant_tables import (
    #         ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
    #         RadiantConfigKeys,
    #         get_config_value,
    #     )
    #
    #     conf = get_current_context()["dag_run"].conf or {}
    #     catalog = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_CATALOG)
    #     database = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_DATABASE)
    #     ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)
    #     # `latest` is a moving tag, so it names no release. Anything else is a dataset_version the
    #     # deployment pinned, and that is exactly what is worth recording.
    #     dataset_version = "" if ref in ("", "latest") else ref
    #     return [
    #         {
    #             "catalog_name": catalog,
    #             "database_name": database,
    #             "table_name": table,
    #             "iceberg_ref": ref,
    #             "dataset_version": dataset_version,
    #         }
    #         for table in sorted(ICEBERG_OPEN_DATA_CONTRACT_MAPPING.values())
    #     ]
    #
    # @task(task_id="render_release_sql", task_display_name="[PyOp] Render Release SQL")
    # def render_release_sql(releases: list[dict[str, str]]) -> str:
    #     import jinja2
    #     from airflow.operators.python import get_current_context
    #
    #     from radiant.dags import DAGS_DIR
    #     from radiant.tasks.data.radiant_tables import get_radiant_mapping
    #
    #     context = get_current_context()
    #     conf = context["dag_run"].conf or {}
    #     text = (DAGS_DIR / "sql" / "radiant" / "open_data_release_insert.sql").read_text()
    #     return jinja2.Template(text).render(
    #         mapping=get_radiant_mapping(conf),
    #         releases=releases,
    #         # Wall-clock, not `logical_date`: the column records when the rebuild finished, and on a
    #         # catch-up or a manual re-run those two are not the same instant. Formatted here rather than
    #         # templated because `{{ ts }}` carries a UTC offset StarRocks' DATETIME will not parse.
    #         recorded_at=datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S"),
    #         dag_run_id=context["run_id"],
    #     )
    #
    # record_release = RadiantStarRocksOperator(
    #     task_id="record_open_data_release",
    #     task_display_name="[StarRocks] P4 - Record the Release",
    #     sql=render_release_sql(build_release_rows()),
    #     # NONE_FAILED, not ALL_SUCCESS: the tenant and part lookups are short-circuits, so on a platform
    #     # with no experiments yet every rebuild branch skips. Under ALL_SUCCESS that would skip P4, which
    #     # would skip the lock release below and strand the mutex on a run where nothing went wrong.
    #     # A real failure still skips this task, and with it the release -- which is the behaviour §4 asks
    #     # for. Same trigger rule, for the same reason, as `import_part`'s final tasks.
    #     trigger_rule=TriggerRule.NONE_FAILED,
    # )

    # --- Flow --------------------------------------------------------------------------------------
    _preflight_tables_exist >> reference_load >> sources_loaded

    # LOCK DISABLED: restore as
    #   _preflight_tables_exist >> _acquire_import_lock >> reference_load >> sources_loaded
    # STAGED ROLLOUT: the release hangs off the checkpoint while P2-P4 are disabled. Move it back to
    # `record_release` as the phases come online -- it must always be downstream of the last live task,
    # or a failure in a phase it does not depend on would still release the mutex.
    # sources_loaded >> _release_import_lock

    # sources_loaded >> [all_tenants, all_parts, tenant_parts]
    # sources_loaded >> tg_accumulators
    # sources_loaded >> tg_cnv
    #
    # Wired statement-to-statement, not group-to-group: a `tg_accumulators >> tg_variants` edge would
    # also make the consequence accumulator a predecessor of the variant chain, and §5's two rows are
    # independent -- the variant chain re-reads `snv__staging_variant`, never `snv__consequence`.
    # reannotate_staging_variant >> insert_snv_variants
    # reannotate_consequence >> insert_consequence_filter
    #
    # [tg_variants, tg_consequences, tg_cnv] >> record_release
    #
    # Release only if every task above succeeded (default trigger_rule=ALL_SUCCESS): a failed or
    # skipped run must leave the lock held.
    # record_release >> _release_import_lock


reannotate_open_data()
