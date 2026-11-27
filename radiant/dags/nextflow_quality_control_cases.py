"""Decide the cases to quality-control, run the pipeline over them, register the result.

`radiant-nextflow-quality-control` runs the Ferlab quality-control-pipeline. This DAG closes the
two manual ends around it, the way `nextflow_snv_postprocessing_cases` does for annotation: it
builds the samplesheet of the named cases, triggers that DAG, then registers what the run
published back onto the cases as one `quality_control_metrics` task each, carrying the
per-family MultiQC set as `aggqc` documents.

    fetch_facts_<pipeline> -> decide_<pipeline> -> journal_<pipeline>      (quality_control, quality_control_somatic)
                 +-> fetch_germline_members -+
    has_cases ---+                           +-> select_cases -> resolve_cases -> generate_inputs -> run_pipeline
                 +-> fetch_somatic_members --+                                                         |
                                            register_tasks (one per tenant) <- collect_outputs <-------+

It decides its own cases (`radiant.dags.pipeline_decision`), one head per case type: `decide_cases` and
`decide_somatic_cases`, on `sql/clinical/quality_control_{,somatic_}facts_select.sql`, journaled as two
pipelines. It moves no status: quality control does not count for the case statuses yet. `case_ids` limits
the run to the cases an operator names. `select_cases` checks the cases decided `run` again,
leniently: a case that cannot be run is excluded with a reason, and a germline member other than
the proband with no CRAM yet is left out of the family. Germline cases bring every family member
present; somatic cases their tumor alone. The DRAGEN metrics
come from the clinical model: each member's samplesheet row carries its alignment's `ssup`/`tgz`
tarball in `dragenMetrics`, or, for germline only, its CRAM's directory when there is none. The
pipeline handles each sample on its own, so every case of the run goes into one launcher run.

Full analysis: `design/SJRA-1879-nextflow-quality-control-automation.md`, and radiant-hub
`docs/features/RAD-110-qc-dragen-metrics-samplesheet.md` for the `dragenMetrics` column.
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
from radiant.dags.pipeline_decision import decision_head
from radiant.tasks.journal import Pipeline
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, rows_output_processor

LOGGER = logging.getLogger(__name__)

# Same workspace as post-processing: the Airflow tasks read and write S3, the pipeline reads
# and writes the FSx mount those buckets are wired to.
INPUTS_ROOT_ENV = "NEXTFLOW_INPUTS_ROOT"
OUTPUTS_ROOT_ENV = "NEXTFLOW_OUTPUTS_ROOT"
DEFAULT_INPUTS_MOUNT = "/workspace/inputs"
DEFAULT_OUTPUTS_MOUNT = "/workspace/outputs"
# The file1 of every somatic row.
PLACEHOLDER_VCF_ENV = "NEXTFLOW_QC_PLACEHOLDER_VCF"
DEFAULT_PLACEHOLDER_VCF = "/workspace/reference/qc/placeholder.vcf.gz"

OUTPUT_POLL_ATTEMPTS = max(1, int(os.getenv("NEXTFLOW_OUTPUT_POLL_ATTEMPTS", "10")))
OUTPUT_POLL_INTERVAL_SECONDS = int(os.getenv("NEXTFLOW_OUTPUT_POLL_INTERVAL", "30"))

PIPELINE_DAG_ID = f"{NAMESPACE}-nextflow-quality-control"

# `select_cases` XCom: what it selected and left out per pipeline, read back by `journal_outcomes`.
SELECTION_KEY = "selection"

# The journal write is idempotent (an unchanged marker is not written twice), so a retry is safe.
JOURNAL_RETRIES = 2
JOURNAL_RETRY_DELAY = datetime.timedelta(minutes=1)

# Under the shared roots, apart from post-processing (`snv-runs/`, `snv/`).
INPUTS_SUBDIR = "qc-runs"
OUTPUTS_SUBDIR = "qc"


dag_params = {
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
    "case_ids": Param(
        [],
        type="array",
        items={"type": "integer"},
        title="Case ids",
        description=(
            "Limit the run to these germline and somatic cases, among the ones decided `run`. Every candidate is "
            "still decided and journaled. Empty runs every case decided `run`."
        ),
    ),
    "dry_run": Param(
        False,
        type="boolean",
        title="Dry run the registration",
        description=(
            "When enabled the batch PATCH validates and writes nothing. A dry run leaves every "
            "case eligible, so a *scheduled* run must not use it."
        ),
    ),
}


def _selection_payload(selection) -> dict:
    return {
        "case_ids": selection.case_ids,
        "members": [m.model_dump() for m in selection.members],
        "excluded": [e.model_dump(mode="json") for e in selection.excluded],
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
        "placeholder": os.getenv(PLACEHOLDER_VCF_ENV, DEFAULT_PLACEHOLDER_VCF),
    }


@dag(
    dag_id=f"{NAMESPACE}-nextflow-quality-control-cases",
    dag_display_name="Radiant - Nextflow Quality Control (from Cases)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    # Started by `radiant-case-scheduler`, which moves the case statuses around it. A
    # schedule of its own would process cases without moving them, and race that DAG.
    schedule=None,
    catchup=False,
    # One run at a time: two runs on the same cases would register two QC tasks each.
    max_active_runs=1,
    tags=["radiant", "nextflow", "qc"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_quality_control_cases.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def nextflow_quality_control_cases():
    decided_germline, _ = decision_head(
        Pipeline.QUALITY_CONTROL,
        "./sql/clinical/quality_control_facts_select.sql",
        "germline QC",
        suffix="_quality_control",
        move_to_processing=False,
    )
    decided_somatic, _ = decision_head(
        Pipeline.QUALITY_CONTROL_SOMATIC,
        "./sql/clinical/quality_control_somatic_facts_select.sql",
        "somatic QC",
        suffix="_quality_control_somatic",
        move_to_processing=False,
    )

    @task.short_circuit(task_id="has_cases", task_display_name="[PyOp] Skip when no case is to run")
    def has_cases(germline: dict, somatic: dict) -> bool:
        return bool(germline["run_case_ids"] or somatic["run_case_ids"])

    def fetch_members(task_id: str, display_name: str, sql: str, decide_task_id: str) -> RadiantStarRocksOperator:
        return RadiantStarRocksOperator(
            task_id=task_id,
            task_display_name=f"[StarRocks] {display_name}",
            sql=sql,
            # `[-1]` when none of this case type runs: the query needs a non-empty list.
            parameters={"case_ids": f"{{{{ ti.xcom_pull(task_ids='{decide_task_id}')['run_case_ids'] or [-1] }}}}"},
            output_processor=rows_output_processor,
            do_xcom_push=True,
        )

    fetch_germline_members = fetch_members(
        "fetch_germline_members",
        "Fetch the members of the germline cases to run",
        "./sql/clinical/quality_control_case_members_select.sql",
        "decide_quality_control",
    )
    fetch_somatic_members = fetch_members(
        "fetch_somatic_members",
        "Fetch the tumor of the somatic cases to run",
        "./sql/clinical/quality_control_somatic_case_members_select.sql",
        "decide_quality_control_somatic",
    )

    @task(task_id="select_cases", task_display_name="[PyOp] Select the cases that can run")
    def select_cases(germline_rows: Any, somatic_rows: Any) -> Any:
        """Keep what can run, report what cannot, and skip the run if nothing can."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.journal import Pipeline
        from radiant.tasks.nextflow.qc.resolve import select_run_cases

        context = get_current_context()
        excluded_tenants = context["params"]["excluded_tenants"]
        selections = {
            Pipeline.QUALITY_CONTROL: select_run_cases(germline_rows, excluded_tenants),
            Pipeline.QUALITY_CONTROL_SOMATIC: select_run_cases(somatic_rows, excluded_tenants),
        }
        # Kept apart from the return value, so the outcomes are journaled even when nothing runs.
        payloads = {str(pipeline): _selection_payload(s) for pipeline, s in selections.items()}
        context["ti"].xcom_push(key=SELECTION_KEY, value=payloads)

        selected = {
            "case_ids": sorted(c for p in payloads.values() for c in p["case_ids"]),
            "members": [m for p in payloads.values() for m in p["members"]],
        }
        if not selected["case_ids"]:
            excluded = sum(len(p["excluded"]) for p in payloads.values())
            raise AirflowSkipException(f"nothing to run: {excluded} named case(s), all excluded")

        LOGGER.info(
            "%d case(s) selected across tenant(s) %s: %s",
            len(selected["case_ids"]),
            ", ".join(sorted({t for s in selections.values() for t in s.tenants})),
            ", ".join(str(case_id) for case_id in selected["case_ids"]),
        )
        return selected

    @task(task_id="resolve_cases", task_display_name="[PyOp] Resolve cases to families")
    def resolve_cases(selected: Any) -> Any:
        from radiant.tasks.nextflow.qc.resolve import resolve_cases as resolve

        return [c.model_dump() for c in resolve(selected["members"])]

    @task(task_id="generate_inputs", task_display_name="[PyOp] Generate the samplesheet")
    def generate_inputs(cases: Any) -> Any:
        """Write the run's samplesheet to its own S3 prefix. FSx auto-imports it, so the
        pipeline sees it on its mount without this task needing the PVC."""
        import boto3
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.paths import run_paths, sanitize_run_tag, split_s3_uri
        from radiant.tasks.nextflow.qc.inputs import build_inputs
        from radiant.tasks.nextflow.qc.model import QcCase

        env = _workspace_env()
        paths = run_paths(
            inputs_root=env["inputs_root"],
            outputs_root=env["outputs_root"],
            inputs_mount=env["inputs_mount"],
            outputs_mount=env["outputs_mount"],
            run_tag=sanitize_run_tag(get_current_context()["run_id"]),
            inputs_subdir=INPUTS_SUBDIR,
            outputs_subdir=OUTPUTS_SUBDIR,
        )
        files = build_inputs([QcCase(**c) for c in cases], env["inputs_root"], env["inputs_mount"], env["placeholder"])

        bucket, prefix = split_s3_uri(paths["input_prefix_s3"])
        # Load-bearing: the next block deletes everything under `prefix` on a bucket shared
        # with every other run's inputs.
        if not prefix.endswith(f"/{paths['run_tag']}") and prefix != paths["run_tag"]:
            raise ValueError(f"refusing to clear {bucket}/{prefix}: it is not this run's own prefix")

        s3 = boto3.client("s3")
        stale = [
            {"Key": obj["Key"]}
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
            for obj in page.get("Contents", [])
        ]
        for start in range(0, len(stale), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": stale[start : start + 1000]})
        for relative_key, content in files.items():
            s3.put_object(Bucket=bucket, Key=f"{prefix}/{relative_key}", Body=content.encode())
        LOGGER.info("wrote %d file(s) to %s", len(files), paths["input_prefix_s3"])
        LOGGER.info("samplesheet:\n%s", files["samplesheet.csv"])

        return paths

    @task(task_id="collect_outputs", task_display_name="[PyOp] Collect pipeline outputs")
    def collect_outputs(cases: Any, paths: Any) -> Any:
        import time

        import boto3

        from radiant.tasks.nextflow.paths import split_s3_uri
        from radiant.tasks.nextflow.qc.model import QcCase
        from radiant.tasks.nextflow.qc.outputs import MissingOutputsError, collect

        parsed = [QcCase(**c) for c in cases]
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
            LOGGER.info("collected outputs for %d case(s)", len(collected))
            return collected
        raise last_error

    @task(task_id="list_tenants", task_display_name="[PyOp] List the tenants to register into")
    def list_tenants(cases: Any) -> Any:
        from radiant.tasks.nextflow.qc.model import QcCase
        from radiant.tasks.nextflow.qc.resolve import tenants_of

        return tenants_of([QcCase(**c) for c in cases])

    @task(
        task_id="register_tasks",
        task_display_name="[PyOp] Register tasks in the portal",
        # PATCH appends: mapped per tenant so a retry never replays a tenant that succeeded.
        max_active_tis_per_dagrun=1,
    )
    def register_tasks(tenant: str, cases: Any, collected: Any) -> Any:
        from airflow.exceptions import AirflowFailException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.qc.batch import build_patch_body
        from radiant.tasks.nextflow.qc.model import QcCase
        from radiant.tasks.nextflow.register import register_case_batch

        context = get_current_context()
        parsed = [QcCase(**c) for c in cases if c["tenant_code"] == tenant]
        if not parsed:
            raise AirflowFailException(f"no resolved case belongs to tenant '{tenant}'")
        body = build_patch_body(parsed, collected)
        return {"tenant": tenant, "report": register_case_batch(tenant, body, context["params"]["dry_run"])}

    # Retried: the run's state ignores this teardown, so a failed write is only seen on its own task.
    @task(
        task_id="journal_outcomes",
        task_display_name="[PyOp] Journal what the run did with its cases",
        retries=JOURNAL_RETRIES,
        retry_delay=JOURNAL_RETRY_DELAY,
    )
    def journal_outcomes() -> int:
        """`succeeded` for the cases of the tenants registered, `failed` for the others and the cases left out."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.journal import Pipeline, record
        from radiant.tasks.nextflow.lineage import with_output_tasks
        from radiant.tasks.nextflow.model import family_id
        from radiant.tasks.nextflow.resolve import outcome_entries

        context = get_current_context()
        if context["params"]["dry_run"]:
            raise AirflowSkipException("dry run: nothing registered, nothing to journal")
        selections = context["ti"].xcom_pull(task_ids="select_cases", key=SELECTION_KEY) or {}
        registered = {r["tenant"] for r in (context["ti"].xcom_pull(task_ids="register_tasks") or []) if r}
        entries = [
            e
            for pipeline, selection in selections.items()
            for e in outcome_entries(selection, registered, Pipeline(pipeline))
        ]
        entries = with_output_tasks(
            entries, context["ti"].xcom_pull(task_ids="collect_outputs"), lambda e: family_id(e.case_id)
        )
        written = record(entries, context["run_id"])
        LOGGER.info("journaled %d outcome(s); tenants registered: %s", len(written), sorted(registered))
        return len(written)

    has_cases(decided_germline, decided_somatic) >> [fetch_germline_members, fetch_somatic_members]
    selection = select_cases(fetch_germline_members.output, fetch_somatic_members.output)
    cases = resolve_cases(selection)
    paths = generate_inputs(cases)

    run_pipeline = TriggerDagRunOperator(
        task_id="run_pipeline",
        task_display_name="[DAG] Run Nextflow Quality Control",
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

    collected = collect_outputs(cases, paths)
    registered = register_tasks.partial(cases=cases, collected=collected).expand(tenant=list_tenants(cases))
    # A teardown: it runs whatever happened before it, and the run's state ignores it, so a failed pipeline or
    # registration still fails the run.
    [selection, registered] >> journal_outcomes().as_teardown()

    paths >> run_pipeline >> collected


nextflow_quality_control_cases()
