"""Run the Ferlab post-processing pipeline from a list of case ids, end to end.

`radiant-nextflow-postprocessing` runs the pipeline. This DAG closes the two manual ends
around it: it builds the samplesheet, PED files and phenopackets from the clinical model,
triggers that DAG, then registers what the run published back onto the cases as
`radiant_germline_annotation` and `exomiser` tasks.

    fetch_members ----+
                      +-> resolve_cases -> generate_inputs -> run_pipeline
    fetch_phenotypes -+                                            |
                                        register_tasks <- collect_outputs

It **triggers** the pipeline DAG rather than duplicating it, so the driver-pod, resume and
cleanup behaviour stay in one place.

Paths are derived from the run, not parameterised: `{root}/{run_tag}` for both inputs and
outputs, where `run_tag` comes from `run_id`. That makes "a fresh prefix per run"
structural rather than a rule someone has to remember, and gives each run its own output
location -- which matters because re-running a case adds a second analysis alongside the
first rather than replacing it.

Full analysis: `design/SJRA-1843-nextflow-postprocessing-from-cases.md`.
"""

import logging
import os
from collections.abc import Sequence
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator

LOGGER = logging.getLogger(__name__)

# The shared workspace has two faces: the Airflow tasks read and write S3, the pipeline
# reads and writes the FSx mount those buckets are wired to. Both halves are named rather
# than one being inferred from the other.
INPUTS_ROOT_ENV = "NEXTFLOW_INPUTS_ROOT"
OUTPUTS_ROOT_ENV = "NEXTFLOW_OUTPUTS_ROOT"
DEFAULT_INPUTS_MOUNT = "/workspace/inputs"
DEFAULT_OUTPUTS_MOUNT = "/workspace/outputs"

# The FSx data-repository association exports on a best-effort drain, not a barrier, so the
# outputs can lag the driver pod's exit by a little.
OUTPUT_POLL_ATTEMPTS = max(1, int(os.getenv("NEXTFLOW_OUTPUT_POLL_ATTEMPTS", "10")))
OUTPUT_POLL_INTERVAL_SECONDS = int(os.getenv("NEXTFLOW_OUTPUT_POLL_INTERVAL", "30"))

PORTAL_CONN_ID = "radiant_api_conn"

dag_params = {
    "case_ids": Param(
        type="array",
        items={"type": "integer"},
        minItems=1,
        title="Case ids",
        description=(
            "`cases.id` values. Germline only; a somatic id is rejected. The tenant is read off "
            "the cases, so they must all belong to the same one."
        ),
    ),
    "dry_run": Param(
        True,
        type="boolean",
        title="Dry run the registration",
        description=(
            "When enabled the batch PATCH validates and writes nothing. Run once this way first: "
            "the report names every failure with its code and path."
        ),
    ),
}


def rows_output_processor(results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
    """Cursor rows to a list of dicts, following `import_radiant.py`."""
    column_names = [desc[0] for desc in descriptions[0]]
    return [[dict(zip(column_names, row, strict=False)) for row in results[0]]]


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
    dag_id=f"{NAMESPACE}-nextflow-postprocessing-cases",
    dag_display_name="Radiant - Nextflow Post-processing (from Cases)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    # The pipeline DAG this triggers is max_active_runs=1 because concurrent drivers would
    # share the FSx workspace. Serialising here too keeps the queue visible on the parent.
    max_active_runs=1,
    tags=["radiant", "nextflow", "manual"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_postprocessing_cases.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def nextflow_postprocessing_cases():
    # The clinical tables are one shared schema behind the `radiant_jdbc` catalog, not one
    # per tenant, and `cases.id` is a single-column primary key over all of it -- so the case
    # ids alone scope the result, and the tenant comes back with the rows. Nothing here
    # needs RADIANT_TENANT_CODE.
    query_parameters = {"case_ids": "{{ params.case_ids }}"}

    fetch_members = RadiantStarRocksOperator(
        task_id="fetch_members",
        task_display_name="[StarRocks] Fetch case members",
        sql="./sql/clinical/case_members_select.sql",
        parameters=query_parameters,
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    fetch_phenotypes = RadiantStarRocksOperator(
        task_id="fetch_phenotypes",
        task_display_name="[StarRocks] Fetch case phenotypes",
        sql="./sql/clinical/case_phenotypes_select.sql",
        parameters=query_parameters,
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="resolve_cases", task_display_name="[PyOp] Resolve cases to families")
    def resolve_cases(member_rows: Any, phenotype_rows: Any) -> Any:
        """Group and validate. Every check here fails the run rather than producing a
        plausible-but-wrong samplesheet, which would only surface hours into the pipeline."""
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.resolve import resolve_families

        context = get_current_context()
        families = resolve_families(member_rows, phenotype_rows, context["params"]["case_ids"])
        LOGGER.info(
            "resolved %d case(s) in tenant '%s': %s",
            len(families),
            families[0].tenant_code,
            ", ".join(f"{f.family_id} ({len(f.members)} member(s), {f.sequencing_type})" for f in families),
        )
        return [f.model_dump() for f in families]

    @task(task_id="generate_inputs", task_display_name="[PyOp] Generate samplesheet, PED, phenopackets")
    def generate_inputs(families: Any) -> Any:
        """Write the run's input prefix on S3. FSx auto-imports it, so the pipeline sees
        the files on its mount without this task needing the PVC."""
        import boto3
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.inputs import build_inputs
        from radiant.tasks.nextflow.model import Family
        from radiant.tasks.nextflow.paths import run_paths, sanitize_run_tag, split_s3_uri

        env = _workspace_env()
        context = get_current_context()
        paths = run_paths(
            inputs_root=env["inputs_root"],
            outputs_root=env["outputs_root"],
            inputs_mount=env["inputs_mount"],
            outputs_mount=env["outputs_mount"],
            run_tag=sanitize_run_tag(context["run_id"]),
        )

        parsed = [Family(**f) for f in families]
        files = build_inputs(parsed, paths["input_prefix_pod"], env["inputs_root"], env["inputs_mount"])

        bucket, prefix = split_s3_uri(paths["input_prefix_s3"])
        s3 = boto3.client("s3")

        # Load-bearing, not defensive style: the next block deletes everything under
        # `prefix` on a bucket shared with every other run's inputs. A prefix that had lost
        # its run tag would take the lot.
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

        from radiant.tasks.nextflow.model import Family
        from radiant.tasks.nextflow.outputs import MissingOutputsError, collect
        from radiant.tasks.nextflow.paths import split_s3_uri

        parsed = [Family(**f) for f in families]
        bucket, prefix = split_s3_uri(paths["outdir_s3"])
        s3 = boto3.client("s3")

        last_error = None
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

    @task(task_id="register_tasks", task_display_name="[PyOp] Register tasks in the portal")
    def register_tasks(families: Any, collected: Any) -> Any:
        import json

        from airflow.exceptions import AirflowFailException, AirflowNotFoundException
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.batch import build_patch_body
        from radiant.tasks.nextflow.model import Family
        from radiant.tasks.nextflow.portal import fetch_token, patch_case_batch, wait_for_batch
        from radiant.tasks.nextflow.resolve import tenant_of

        context = get_current_context()
        dry_run = context["params"]["dry_run"]
        parsed = [Family(**f) for f in families]
        # The tenant the cases themselves carry, not one someone typed at trigger time --
        # so the batch can only ever be addressed where the data actually came from.
        tenant = tenant_of(parsed)

        try:
            conn = BaseHook.get_connection(PORTAL_CONN_ID)
        except AirflowNotFoundException:
            raise AirflowFailException(
                f"Connection '{PORTAL_CONN_ID}' is not configured. It needs host = the API url, "
                f"login = the OIDC client id, password = its secret, and extra = "
                f'{{"token_url": "...", "scope": "..."}}.'
            ) from None

        extra = conn.extra_dejson
        if not extra.get("token_url"):
            raise AirflowFailException(f"Connection '{PORTAL_CONN_ID}' extra is missing 'token_url'.")

        body = build_patch_body(parsed, collected)
        n_tasks = sum(len(c["tasks"]) for c in body["cases"])
        n_docs = sum(len(t["output_documents"]) for c in body["cases"] for t in c["tasks"])
        LOGGER.info(
            "%s into tenant '%s': %d case(s), %d task(s), %d output document(s)",
            "dry run" if dry_run else "registering",
            tenant,
            len(body["cases"]),
            n_tasks,
            n_docs,
        )

        token = fetch_token(extra["token_url"], conn.login, conn.password, extra.get("scope"))
        batch_id = patch_case_batch(conn.host, tenant, token, body, dry_run)
        LOGGER.info("batch id: %s", batch_id)
        if not batch_id:
            return None

        report = wait_for_batch(conn.host, tenant, token, batch_id)
        LOGGER.info("batch report:\n%s", json.dumps(report, indent=2, default=str)[:8000])
        if str(report.get("status", "")).lower() not in ("success", "succeeded", "completed", "done"):
            raise AirflowFailException(f"batch {batch_id} did not succeed: status={report.get('status')}")
        return report

    families = resolve_cases(fetch_members.output, fetch_phenotypes.output)
    paths = generate_inputs(families)

    run_pipeline = TriggerDagRunOperator(
        task_id="run_pipeline",
        task_display_name="[DAG] Run Nextflow Post-processing",
        trigger_dag_id=f"{NAMESPACE}-nextflow-postprocessing",
        # Pinned, not auto-generated. Without this a retry of *this* DAG would create a
        # fresh child run with a fresh RUN_TAG and therefore a fresh Nextflow launch dir,
        # so `-resume` would find nothing and a retry would become a full re-run.
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
    register_tasks(families, collected)

    paths >> run_pipeline >> collected


nextflow_postprocessing_cases()
