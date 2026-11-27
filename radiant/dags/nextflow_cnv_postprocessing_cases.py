"""Decide the germline cases to CNV post-process, run the pipeline over them, register the result.

`radiant-nextflow-cnv-postprocessing` runs the Ferlab cnv-post-processing pipeline. This DAG
closes the two manual ends around it, the way `nextflow_snv_postprocessing_cases` does for SNV
annotation: it builds the samplesheet, PED files and phenopackets of the named cases from the
clinical model, triggers that DAG, then registers what the run published back onto the cases as
a `radiant_germline_cnv_annotation` task, with the Exomiser reports (data type `exomiser_cnv`)
among its outputs.

    fetch_facts -> decide -> journal, set_processing (one per tenant)
                -> has_cases -> fetch_case_members -> select_cases -> fetch_phenotypes -> resolve_cases
                                                                                                |
                                                                     generate_inputs -> run_pipeline
                                                                                                |
                            register_tasks (one per tenant) <- collect_outputs <----------------+

It decides its own cases (`radiant.dags.pipeline_decision`): every candidate of
`sql/clinical/germline_cnv_facts_select.sql`, decided by `decide_cases` and journaled. The cases decided `run`
move to `processing`, then run; `case_ids` limits the run to the cases an operator names. `select_cases` checks
them again, leniently: a case that cannot be run is
excluded with a reason, and a member other than the proband with no CNV VCF yet is left out of
the family. The trigger document is the current alignment's `gcnv`/`vcf`; its CRAM rides along
and is passed to the depth-based genotype refinement only when every member of the family has
one with its index at `<cram>.crai`.

Full analysis: `design/cnv-post-processing-nextflow-integration.md`.
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

# Same workspace as the other Nextflow DAGs: the Airflow tasks read and write S3, the pipeline
# reads and writes the FSx mount those buckets are wired to.
INPUTS_ROOT_ENV = "NEXTFLOW_INPUTS_ROOT"
OUTPUTS_ROOT_ENV = "NEXTFLOW_OUTPUTS_ROOT"
DEFAULT_INPUTS_MOUNT = "/workspace/inputs"
DEFAULT_OUTPUTS_MOUNT = "/workspace/outputs"

OUTPUT_POLL_ATTEMPTS = max(1, int(os.getenv("NEXTFLOW_OUTPUT_POLL_ATTEMPTS", "10")))
OUTPUT_POLL_INTERVAL_SECONDS = int(os.getenv("NEXTFLOW_OUTPUT_POLL_INTERVAL", "30"))

PIPELINE_DAG_ID = f"{NAMESPACE}-nextflow-cnv-postprocessing"

# `select_cases` XCom: what it selected and left out, read back by `journal_outcomes`.
SELECTION_KEY = "selection"

# The journal write is idempotent (an unchanged marker is not written twice), so a retry is safe.
JOURNAL_RETRIES = 2
JOURNAL_RETRY_DELAY = datetime.timedelta(minutes=1)

# Under the shared roots, apart from post-processing (`snv-runs/`, `snv/`)
# and quality control (`qc-runs/`, `qc/`).
INPUTS_SUBDIR = "cnv-runs"
OUTPUTS_SUBDIR = "cnv"


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
            "Limit the run to these cases, among the ones decided `run`. Every candidate is still decided and "
            "journaled. Empty runs every case decided `run`."
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
    }


@dag(
    dag_id=f"{NAMESPACE}-nextflow-cnv-postprocessing-cases",
    dag_display_name="Radiant - Nextflow CNV Post-processing (from Cases)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    # Started by `radiant-case-scheduler`, which moves the case statuses around it. A
    # schedule of its own would process cases without moving them, and race that DAG.
    schedule=None,
    catchup=False,
    # The entire concurrency story: discovery runs at the start of a run, so a queued run
    # re-queries after the previous one has registered and sees the shorter list.
    max_active_runs=1,
    tags=["radiant", "nextflow", "cnv"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_cnv_postprocessing_cases.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def nextflow_cnv_postprocessing_cases():
    decided, processing = decision_head(
        Pipeline.GERMLINE_CNV, "./sql/clinical/germline_cnv_facts_select.sql", "germline CNV"
    )

    @task.short_circuit(task_id="has_cases", task_display_name="[PyOp] Skip when no case is to run")
    def has_cases(decided: dict) -> bool:
        return bool(decided["run_case_ids"])

    fetch_case_members = RadiantStarRocksOperator(
        task_id="fetch_case_members",
        task_display_name="[StarRocks] Fetch the members of the cases to run",
        sql="./sql/clinical/germline_cnv_case_members_select.sql",
        parameters={"case_ids": "{{ ti.xcom_pull(task_ids='decide')['run_case_ids'] }}"},
        # A failed status move must not hold the data back: the run still goes red on it.
        trigger_rule="all_done",
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="select_cases", task_display_name="[PyOp] Select the cases that can run")
    def select_cases(member_rows: Any) -> Any:
        """Keep what can run, report what cannot, and skip the run if nothing can."""
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.cnv.resolve import select_run_cases

        context = get_current_context()
        selection = select_run_cases(member_rows, context["params"]["excluded_tenants"])
        # Kept apart from the return value, so the outcomes are journaled even when nothing runs.
        context["ti"].xcom_push(key=SELECTION_KEY, value=_selection_payload(selection))
        if not selection.case_ids:
            raise AirflowSkipException(f"nothing to run: {len(selection.excluded)} named case(s), all excluded")

        LOGGER.info(
            "%d case(s) selected across tenant(s) %s: %s",
            len(selection.case_ids),
            ", ".join(selection.tenants),
            ", ".join(str(case_id) for case_id in selection.case_ids),
        )
        return _selection_payload(selection)

    # The cases are `processing` before their run starts.
    [has_cases(decided), *processing] >> fetch_case_members
    selection = select_cases(fetch_case_members.output)

    fetch_phenotypes = RadiantStarRocksOperator(
        task_id="fetch_phenotypes",
        task_display_name="[StarRocks] Fetch case phenotypes",
        sql="./sql/clinical/case_phenotypes_select.sql",
        parameters={"case_ids": "{{ ti.xcom_pull(task_ids='select_cases')['case_ids'] }}"},
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="resolve_cases", task_display_name="[PyOp] Resolve cases to families")
    def resolve_cases(selected: Any, phenotype_rows: Any) -> Any:
        from radiant.tasks.nextflow.cnv.resolve import resolve_families

        families = resolve_families(selected["members"], phenotype_rows)
        LOGGER.info(
            "resolved %d case(s) in tenant(s) %s: %s",
            len(families),
            ", ".join(sorted({f.tenant_code for f in families})),
            ", ".join(
                f"{f.family_id} ({len(f.members)} member(s), {f.sequencing_type}, "
                f"{'with' if f.crams_complete else 'without'} CRAMs)"
                for f in families
            ),
        )
        return [f.model_dump() for f in families]

    @task(task_id="list_tenants", task_display_name="[PyOp] List the tenants to register into")
    def list_tenants(families: Any) -> Any:
        """One batch PATCH per tenant, and one mapped `register_tasks` per batch: PATCH appends,
        so a retried loop would double-register every tenant that had already succeeded."""
        from radiant.tasks.nextflow.cnv.model import CnvFamily
        from radiant.tasks.nextflow.cnv.resolve import tenants_of

        return tenants_of([CnvFamily(**f) for f in families])

    @task(task_id="generate_inputs", task_display_name="[PyOp] Generate samplesheet, PED, phenopackets")
    def generate_inputs(families: Any) -> Any:
        """Write the run's input prefix on S3. FSx auto-imports it, so the pipeline sees
        the files on its mount without this task needing the PVC."""
        import boto3
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.cnv.inputs import build_inputs
        from radiant.tasks.nextflow.cnv.model import CnvFamily
        from radiant.tasks.nextflow.paths import run_paths, sanitize_run_tag, split_s3_uri

        env = _workspace_env()
        context = get_current_context()
        paths = run_paths(
            inputs_root=env["inputs_root"],
            outputs_root=env["outputs_root"],
            inputs_mount=env["inputs_mount"],
            outputs_mount=env["outputs_mount"],
            run_tag=sanitize_run_tag(context["run_id"]),
            inputs_subdir=INPUTS_SUBDIR,
            outputs_subdir=OUTPUTS_SUBDIR,
        )

        parsed = [CnvFamily(**f) for f in families]
        files = build_inputs(parsed, paths["input_prefix_pod"], env["inputs_root"], env["inputs_mount"])

        bucket, prefix = split_s3_uri(paths["input_prefix_s3"])
        s3 = boto3.client("s3")

        # Load-bearing, not defensive style: the next block deletes everything under
        # `prefix` on a bucket shared with every other run's inputs.
        if not prefix.endswith(f"/{paths['run_tag']}") and prefix != paths["run_tag"]:
            raise ValueError(f"refusing to clear {bucket}/{prefix}: it is not this run's own prefix")

        # Clear the prefix first: on a retry a regenerated set must never sit beside a
        # stale one, and a leftover PED is exactly the kind of file nothing complains about.
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
    def collect_outputs(families: Any, paths: Any) -> Any:
        import time

        import boto3

        from radiant.tasks.nextflow.cnv.model import CnvFamily
        from radiant.tasks.nextflow.cnv.outputs import MissingOutputsError, collect
        from radiant.tasks.nextflow.paths import split_s3_uri

        parsed = [CnvFamily(**f) for f in families]
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
            LOGGER.info("collected outputs for %d family(ies)", len(collected))
            return collected
        raise last_error

    @task(
        task_id="register_tasks",
        task_display_name="[PyOp] Register tasks in the portal",
        # PATCH appends: mapped per tenant so a retry never replays a tenant that succeeded.
        max_active_tis_per_dagrun=1,
    )
    def register_tasks(tenant: str, families: Any, collected: Any) -> Any:
        from airflow.exceptions import AirflowFailException
        from airflow.sdk import get_current_context

        from radiant.tasks.nextflow.cnv.batch import build_patch_body
        from radiant.tasks.nextflow.cnv.model import CnvFamily
        from radiant.tasks.nextflow.register import register_case_batch

        context = get_current_context()
        # This tenant's share of the run. The tenant comes off the cases themselves, so a
        # batch can only ever be addressed where its data actually came from.
        parsed = [CnvFamily(**f) for f in families if f["tenant_code"] == tenant]
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
        selection = context["ti"].xcom_pull(task_ids="select_cases", key=SELECTION_KEY) or {}
        registered = {r["tenant"] for r in (context["ti"].xcom_pull(task_ids="register_tasks") or []) if r}
        entries = with_output_tasks(
            outcome_entries(selection, registered, Pipeline.GERMLINE_CNV),
            context["ti"].xcom_pull(task_ids="collect_outputs"),
            lambda e: family_id(e.case_id),
        )
        written = record(entries, context["run_id"])
        LOGGER.info("journaled %d outcome(s); tenants registered: %s", len(written), sorted(registered))
        return len(written)

    families = resolve_cases(selection, fetch_phenotypes.output)
    paths = generate_inputs(families)

    run_pipeline = TriggerDagRunOperator(
        task_id="run_pipeline",
        task_display_name="[DAG] Run Nextflow CNV Post-processing",
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

    collected = collect_outputs(families, paths)
    registered = register_tasks.partial(families=families, collected=collected).expand(tenant=list_tenants(families))
    # A teardown: it runs whatever happened before it, and the run's state ignores it, so a failed pipeline or
    # registration still fails the run.
    [selection, registered] >> journal_outcomes().as_teardown()

    selection >> fetch_phenotypes
    paths >> run_pipeline >> collected


nextflow_cnv_postprocessing_cases()
