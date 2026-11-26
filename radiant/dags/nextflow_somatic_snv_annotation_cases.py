"""Annotate the named somatic calling tasks with VEP, register the result.

QLIN sends the raw somatic SNV VCF of each calling task (`somatic_variant_calling`,
`tumor_only_variant_calling`, `alignment_somatic_variant_calling`). This DAG annotates it with the
same VEP as germline -- `radiant-nextflow-somatic-snv-annotation`, the germline pipeline entered at
its `normalize` step with `tools = vep` -- and registers the result on
the case as a `radiant_somatic_annotation` task, which the scheduled import already loads.

    has_tasks -> fetch_tasks -> select_tasks -> generate_inputs -> run_pipeline -> collect_outputs
                                     |                                                   |
                                list_tenants --------------> register_tasks (one per tenant)

The unit of work is the calling task, not the case: a tumor-normal and a tumor-only call on the same
tumor sample are both annotated. It decides nothing: `radiant-case-scheduler` decides which calling tasks
run (`decide_tasks`, on `sql/clinical/somatic_snv_facts_select.sql`) and passes them as `task_ids`; an
operator can name tasks to rerun. The same query reads them, for their (case, task) pairs not annotated
yet, and `select_tasks` checks them again, leniently. No phenotypes, no PED, no Exomiser.

Design: radiant-hub `docs/features/RAD-23-somatic-vep-annotation.md`.
"""

import datetime
import logging
import os
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from radiant.dags import DEFAULT_ARGS, EXCLUDED_TENANTS_ENV, NAMESPACE, excluded_tenants_from_env, load_docs_md
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, rows_output_processor

LOGGER = logging.getLogger(__name__)

# Same workspace as the other Nextflow DAGs: the Airflow tasks read and write S3, the pipeline
# reads and writes the FSx mount those buckets are wired to.
INPUTS_ROOT_ENV = "NEXTFLOW_INPUTS_ROOT"
OUTPUTS_ROOT_ENV = "NEXTFLOW_OUTPUTS_ROOT"
DEFAULT_INPUTS_MOUNT = "/workspace/inputs"
DEFAULT_OUTPUTS_MOUNT = "/workspace/outputs"

OUTPUT_POLL_ATTEMPTS = max(1, int(os.getenv("NEXTFLOW_OUTPUT_POLL_ATTEMPTS", "10")))
OUTPUT_POLL_INTERVAL_SECONDS = int(os.getenv("NEXTFLOW_OUTPUT_POLL_INTERVAL", "30"))

PIPELINE_DAG_ID = f"{NAMESPACE}-nextflow-somatic-snv-annotation"

# Under the shared roots, apart from germline post-processing (`snv-runs/`, `snv/`), CNV
# (`cnv-runs/`, `cnv/`) and quality control (`qc-runs/`, `qc/`).
INPUTS_SUBDIR = "somatic-snv-runs"
OUTPUTS_SUBDIR = "somatic-snv"

# `select_tasks` XCom: what it selected and left out, read back by `journal_outcomes`.
SELECTION_KEY = "selection"

# The journal write is idempotent (an unchanged marker is not written twice), so a retry is safe.
JOURNAL_RETRIES = 2
JOURNAL_RETRY_DELAY = datetime.timedelta(minutes=1)

# Load-bearing, not cosmetic. The case scheduler pins the same run id on every cases DAG it
# triggers, and the Nextflow launch dir and work dir are keyed by the run tag on a filesystem every
# launcher shares. Without the prefix, this run and the germline SNV one would share both, and each
# one's cleanup would delete the other's scratch.
RUN_TAG_PREFIX = "somatic-"


dag_params = {
    "task_ids": Param(
        [],
        type="array",
        items={"type": "integer"},
        title="Calling task ids",
        description=(
            "The `somatic_variant_calling`, `tumor_only_variant_calling` or `alignment_somatic_variant_calling` "
            "tasks to run: `radiant-case-scheduler` passes the ones it decided `run` for, and an operator can "
            "name tasks to rerun, e.g. [70]. Each runs for its cases not annotated yet. Empty runs nothing."
        ),
    ),
    "excluded_tenants": Param(
        excluded_tenants_from_env(),
        type="array",
        items={"type": "string"},
        title="Excluded tenants",
        description=(
            "Tenants left out of case processing: their cases are excluded before the pipeline "
            f"runs, as `tenant_excluded`. Defaults to ${EXCLUDED_TENANTS_ENV}; empty excludes none. "
            "The case scheduler passes its own."
        ),
    ),
    "dry_run": Param(
        False,
        type="boolean",
        title="Dry run the registration",
        description=(
            "When enabled the batch PATCH validates and writes nothing. A dry run leaves every "
            "calling task pending, so a *scheduled* run must not use it."
        ),
    ),
}


def _workspace_env() -> dict[str, str]:
    inputs_root = os.getenv(INPUTS_ROOT_ENV)
    outputs_root = os.getenv(OUTPUTS_ROOT_ENV)
    if not inputs_root or not outputs_root:
        raise ValueError(f"{INPUTS_ROOT_ENV} and {OUTPUTS_ROOT_ENV} must both be set to an s3:// uri")
    return {
        "inputs_root": inputs_root,
        "outputs_root": outputs_root,
        "inputs_mount": os.getenv("NEXTFLOW_INPUTS_MOUNT", DEFAULT_INPUTS_MOUNT),
        "outputs_mount": os.getenv("NEXTFLOW_OUTPUTS_MOUNT", DEFAULT_OUTPUTS_MOUNT),
    }


@dag(
    dag_id=f"{NAMESPACE}-nextflow-somatic-snv-annotation-cases",
    dag_display_name="Radiant - Nextflow Somatic SNV Annotation (from Cases)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    # Started by `radiant-case-scheduler`, which moves the case statuses around it. A
    # schedule of its own would process cases without moving them, and race that DAG.
    schedule=None,
    catchup=False,
    # One run at a time: a queued run reads the named tasks after the previous one has registered.
    max_active_runs=1,
    tags=["radiant", "nextflow", "somatic"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_somatic_snv_annotation_cases.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def nextflow_somatic_snv_annotation_cases():
    @task.short_circuit(task_id="has_tasks", task_display_name="[PyOp] Skip when no calling task is named")
    def has_tasks() -> bool:
        from airflow.sdk import get_current_context

        return bool(get_current_context()["params"]["task_ids"])

    fetch_tasks = RadiantStarRocksOperator(
        task_id="fetch_tasks",
        task_display_name="[StarRocks] Fetch the named calling tasks",
        sql="./sql/clinical/somatic_snv_facts_select.sql",
        parameters={"task_ids": "{{ params.task_ids }}"},
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="select_tasks", task_display_name="[PyOp] Select the calling tasks that can run")
    def select_tasks(rows: Any) -> Any:
        """Keep what can run, report what cannot, and skip the run if nothing can."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.somatic.resolve import select_run_tasks

        context = get_current_context()
        selection = select_run_tasks(rows, context["params"]["excluded_tenants"])
        # Kept apart from the return value, so the outcomes are journaled even when nothing runs.
        context["ti"].xcom_push(
            key=SELECTION_KEY,
            value={
                "tasks": [t.model_dump() for t in selection.tasks],
                "excluded": [e.model_dump(mode="json") for e in selection.excluded],
            },
        )
        if not selection.tasks:
            raise AirflowSkipException(f"nothing to run: {len(selection.excluded)} named task(s) or case(s) excluded")

        LOGGER.info(
            "%d calling task(s) selected for %d case(s) across tenant(s) %s: %s",
            len(selection.tasks),
            len(selection.case_ids),
            ", ".join(selection.tenants),
            ", ".join(
                f"{t.task_id} ({t.task_type}, {'TN' if t.normal_aliquot else 'TO'}, "
                f"cases {[c.case_id for c in t.cases]})"
                for t in selection.tasks
            ),
        )
        return [t.model_dump() for t in selection.tasks]

    @task(task_id="list_tenants", task_display_name="[PyOp] List the tenants to register into")
    def list_tenants(tasks: Any) -> Any:
        """One batch PATCH per tenant, and one mapped `register_tasks` per batch: PATCH appends,
        so a retried loop would double-register every tenant that had already succeeded."""
        from radiant.tasks.nextflow.somatic.model import CallingTask
        from radiant.tasks.nextflow.somatic.resolve import tenants_of

        return tenants_of([CallingTask(**t) for t in tasks])

    @task(task_id="generate_inputs", task_display_name="[PyOp] Generate the samplesheet")
    def generate_inputs(tasks: Any) -> Any:
        """Write the run's input prefix on S3. FSx auto-imports it, so the pipeline sees
        the samplesheet on its mount without this task needing the PVC."""
        import boto3
        from airflow.sdk import get_current_context
        from botocore.exceptions import ClientError

        from radiant.tasks.nextflow.paths import run_paths, sanitize_run_tag, split_s3_uri
        from radiant.tasks.nextflow.somatic.inputs import build_inputs
        from radiant.tasks.nextflow.somatic.model import CallingTask

        env = _workspace_env()
        context = get_current_context()
        paths = run_paths(
            inputs_root=env["inputs_root"],
            outputs_root=env["outputs_root"],
            inputs_mount=env["inputs_mount"],
            outputs_mount=env["outputs_mount"],
            run_tag=RUN_TAG_PREFIX + sanitize_run_tag(context["run_id"]),
            inputs_subdir=INPUTS_SUBDIR,
            outputs_subdir=OUTPUTS_SUBDIR,
        )

        parsed = [CallingTask(**t) for t in tasks]
        files = build_inputs(parsed, env["inputs_root"], env["inputs_mount"])

        s3 = boto3.client("s3")
        # The pipeline stats every VCF at launch and fails the whole run on the first missing one.
        # Checked here instead, naming every calling task and URL at once, before any pod starts.
        missing = []
        for calling_task in parsed:
            vcf_bucket, vcf_key = split_s3_uri(calling_task.vcf_url)
            try:
                s3.head_object(Bucket=vcf_bucket, Key=vcf_key)
            except ClientError as error:
                # Only a 404 means "not found": a 403 or a throttle is an infrastructure error, and
                # reporting it as a missing VCF would send someone to fix the wrong thing.
                if error.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                    raise
                missing.append(f"calling task {calling_task.task_id}: {calling_task.vcf_url}")
        if missing:
            raise FileNotFoundError("raw somatic VCF(s) not found, nothing run:\n  " + "\n  ".join(missing))

        bucket, prefix = split_s3_uri(paths["input_prefix_s3"])
        # Load-bearing, not defensive style: the next block deletes everything under
        # `prefix` on a bucket shared with every other run's inputs.
        if not prefix.endswith(f"/{paths['run_tag']}") and prefix != paths["run_tag"]:
            raise ValueError(f"refusing to clear {bucket}/{prefix}: it is not this run's own prefix")

        # Clear the prefix first: on a retry a regenerated set must never sit beside a stale one.
        stale = [
            {"Key": obj["Key"]}
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
            for obj in page.get("Contents", [])
        ]
        for start in range(0, len(stale), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": stale[start : start + 1000]})
        if stale:
            LOGGER.info("removed %d stale object(s) under s3://%s/%s/", len(stale), bucket, prefix)

        for relative_key, content in files.items():
            s3.put_object(Bucket=bucket, Key=f"{prefix}/{relative_key}", Body=content.encode())
        LOGGER.info("wrote %d file(s) to %s", len(files), paths["input_prefix_s3"])
        LOGGER.info("samplesheet:\n%s", files["samplesheet.csv"])

        return paths

    @task(task_id="collect_outputs", task_display_name="[PyOp] Collect pipeline outputs")
    def collect_outputs(tasks: Any, paths: Any) -> Any:
        import time

        import boto3

        from radiant.tasks.nextflow.paths import split_s3_uri
        from radiant.tasks.nextflow.somatic.model import CallingTask
        from radiant.tasks.nextflow.somatic.outputs import MissingOutputsError, collect

        parsed = [CallingTask(**t) for t in tasks]
        bucket, prefix = split_s3_uri(paths["outdir_s3"])
        s3 = boto3.client("s3")

        last_error = None
        # The FSx export is a best-effort drain, not a barrier, so the outputs can lag the
        # driver pod's exit by a little.
        for attempt in range(1, OUTPUT_POLL_ATTEMPTS + 1):
            listing = {
                obj["Key"][len(prefix) + 1 :]: obj["Size"]
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
                for obj in page.get("Contents", [])
            }
            try:
                collected = collect(parsed, listing, paths["outdir_s3"])
            except MissingOutputsError as error:
                last_error = error
                LOGGER.warning("attempt %d/%d: %s", attempt, OUTPUT_POLL_ATTEMPTS, error)
                if attempt < OUTPUT_POLL_ATTEMPTS:
                    time.sleep(OUTPUT_POLL_INTERVAL_SECONDS)
                continue
            LOGGER.info("collected outputs for %d calling task(s)", len(collected))
            return collected
        raise last_error

    @task(
        task_id="register_tasks",
        task_display_name="[PyOp] Register tasks in the portal",
        # PATCH appends: mapped per tenant so a retry never replays a tenant that succeeded.
        max_active_tis_per_dagrun=1,
    )
    def register_tasks(tenant: str, tasks: Any, collected: Any) -> Any:
        from airflow.exceptions import AirflowFailException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.register import register_case_batch
        from radiant.tasks.nextflow.somatic.batch import build_patch_body
        from radiant.tasks.nextflow.somatic.model import CallingTask

        context = get_current_context()
        body = build_patch_body([CallingTask(**t) for t in tasks], collected, tenant)
        if not body["cases"]:
            raise AirflowFailException(f"no selected case belongs to tenant '{tenant}'")
        return {"tenant": tenant, "report": register_case_batch(tenant, body, context["params"]["dry_run"])}

    # Retried: the run's state ignores this teardown, so a failed write is only seen on its own task.
    @task(
        task_id="journal_outcomes",
        task_display_name="[PyOp] Journal what the run did with its calling tasks",
        retries=JOURNAL_RETRIES,
        retry_delay=JOURNAL_RETRY_DELAY,
    )
    def journal_outcomes() -> int:
        """`succeeded` for the (case, task) pairs of the tenants registered, `failed` for the others and those left
        out."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.journal import record
        from radiant.tasks.nextflow.lineage import with_output_tasks
        from radiant.tasks.nextflow.somatic.resolve import outcome_entries

        context = get_current_context()
        if context["params"]["dry_run"]:
            raise AirflowSkipException("dry run: nothing registered, nothing to journal")
        selection = context["ti"].xcom_pull(task_ids="select_tasks", key=SELECTION_KEY) or {}
        registered = {r["tenant"] for r in (context["ti"].xcom_pull(task_ids="register_tasks") or []) if r}
        # The outputs are keyed by calling task, the item's key.
        entries = with_output_tasks(
            outcome_entries(selection, registered),
            context["ti"].xcom_pull(task_ids="collect_outputs"),
            lambda e: e.item_key,
        )
        written = record(entries, context["run_id"])
        LOGGER.info("journaled %d outcome(s); tenants registered: %s", len(written), sorted(registered))
        return len(written)

    has_tasks() >> fetch_tasks
    tasks = select_tasks(fetch_tasks.output)
    paths = generate_inputs(tasks)

    run_pipeline = TriggerDagRunOperator(
        task_id="run_pipeline",
        task_display_name="[DAG] Run Nextflow Somatic SNV Annotation",
        trigger_dag_id=PIPELINE_DAG_ID,
        # Pinned, not auto-generated: a retry of *this* DAG re-enters the same child run and
        # its Nextflow launch dir, so `-resume` finds what already completed.
        trigger_run_id="{{ ti.xcom_pull(task_ids='generate_inputs')['run_tag'] }}",
        conf={
            "input": "{{ ti.xcom_pull(task_ids='generate_inputs')['samplesheet_pod'] }}",
            "outdir": "{{ ti.xcom_pull(task_ids='generate_inputs')['outdir_pod'] }}",
        },
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=True,
        poke_interval=60,
    )

    collected = collect_outputs(tasks, paths)
    registered = register_tasks.partial(tasks=tasks, collected=collected).expand(tenant=list_tenants(tasks))
    # A teardown: it runs whatever happened before it, and the run's state ignores it, so a failed pipeline or
    # registration still fails the run.
    [tasks, registered] >> journal_outcomes().as_teardown()

    paths >> run_pipeline >> collected


nextflow_somatic_snv_annotation_cases()
