"""Find the germline cases waiting for an annotation, run the pipeline, register the result.

`radiant-nextflow-postprocessing` runs the Ferlab post-processing pipeline. This DAG closes
the two manual ends around it: it discovers what needs running, builds the samplesheet, PED
files and phenopackets from the clinical model, triggers that DAG, then registers what the
run published back onto the cases as `radiant_germline_annotation` and `exomiser` tasks.

    discover_scope -> select_cases -> fetch_phenotypes -> resolve_cases -> generate_inputs
                                                                                  |
                                                                            run_pipeline
                                                                                  |
              register_tasks (one per tenant) <- collect_outputs <----------------+

Nothing is asked of an operator on a scheduled run. `discover_scope` asks the clinical model
which sequencing has been aligned but never annotated; `task_ids` narrows that to specific
alignment tasks when someone wants a targeted rerun, and is empty otherwise.

Two rules do most of the work, and both live in
`sql/clinical/pending_annotation_select.sql` rather than here:

- exactly one sequencing experiment per (case, member) -- the newest `completed` one -- and
  exactly one alignment task per experiment. The same selection decides eligibility *and*
  builds the family, which is what stops a superseded experiment keeping its case eligible
  for ever;
- a candidate that cannot be run is excluded with a reason and reported, never fatal. One
  unfixable case must not block every other case, every night.

Paths are derived from the run, not parameterised: `{root}/{run_tag}` for both inputs and
outputs, where `run_tag` comes from `run_id`. That gives each run its own output location --
which matters because re-running a case adds a second analysis alongside the first rather
than replacing it.

Full analysis: `design/SJRA-1698-nextflow-postprocessing-automation.md`, which builds on
`design/SJRA-1843-nextflow-postprocessing-from-cases.md`.
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

# Under the shared roots, apart from quality control (`qc-runs/`, `qc/`).
INPUTS_SUBDIR = "postprocessing-runs"
OUTPUTS_SUBDIR = "postprocessing"

# Tenants the service account has been granted `ingest_data` on, comma separated. Empty
# means "do not filter", which is right for a single-tenant deployment and wrong the moment
# a second tenant exists: an ungranted tenant's cases get a flat 403 from the batch PATCH
# *after* the pipeline has spent hours on them, and stay eligible, so the next run does it
# again. Filtering here turns that into a line in the exclusion report.
TENANTS_ENV = "NEXTFLOW_POSTPROCESSING_TENANTS"


def _default_tenants() -> list[str]:
    return [tenant.strip() for tenant in os.getenv(TENANTS_ENV, "").split(",") if tenant.strip()]


dag_params = {
    "task_ids": Param(
        [],
        type="array",
        items={"type": "integer"},
        title="Alignment task ids",
        description=(
            "`alignment_germline_variant_calling` task ids, for a targeted rerun. Leave empty -- "
            "as a scheduled run does -- to process every case that has been aligned but never "
            "annotated. A task shared by several cases runs all of them."
        ),
    ),
    "tenants": Param(
        _default_tenants(),
        type="array",
        items={"type": "string"},
        title="Tenant allow-list",
        description=(
            "Tenants the portal has granted this service account `ingest_data` on. Cases in any "
            "other tenant are excluded before the pipeline runs, rather than failing at "
            f"registration. Defaults to ${TENANTS_ENV}; empty means no filtering."
        ),
    ),
    "dry_run": Param(
        False,
        type="boolean",
        title="Dry run the registration",
        description=(
            "When enabled the batch PATCH validates and writes nothing, and the report names "
            "every failure with its code and path. Note that a dry run leaves every case "
            "eligible, so a *scheduled* run must not use it."
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
    schedule="@daily",
    catchup=False,
    # This is the entire concurrency story, and it is why no lock or marker column is
    # needed. A run that overruns a day makes the next one queue rather than start, and
    # because discovery runs at the *start* of a run, the queued one re-queries after the
    # previous has registered and sees the shorter list. Nothing is picked up twice.
    max_active_runs=1,
    tags=["radiant", "nextflow"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_postprocessing_cases.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def nextflow_postprocessing_cases():
    # The clinical tables are one shared schema behind the `radiant_jdbc` catalog, not one
    # per tenant, and `cases.id` is a single-column primary key over all of it -- so the
    # tenant comes back with the rows rather than scoping the query. Nothing here needs
    # RADIANT_TENANT_CODE.
    discover_scope = RadiantStarRocksOperator(
        task_id="discover_scope",
        task_display_name="[StarRocks] Discover cases pending annotation",
        sql="./sql/clinical/pending_annotation_select.sql",
        parameters={
            "task_ids": "{{ params.task_ids }}",
            "tenants": "{{ params.tenants }}",
        },
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="select_cases", task_display_name="[PyOp] Select the cases that can run")
    def select_cases(member_rows: Any) -> Any:
        """Keep what can run, report what cannot, and skip the run if nothing can."""
        from airflow.exceptions import AirflowSkipException
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.resolve import select_cases as select

        context = get_current_context()
        requested = list(context["params"]["task_ids"] or [])
        # Named tasks are an operator's request, so a case that cannot be resolved is an
        # error. A discovered scope is the job's own doing, so it is a line in a report.
        selection = select(member_rows, requested, strict=bool(requested))

        if not selection.case_ids:
            raise AirflowSkipException(
                f"nothing to run: {len(selection.excluded)} candidate case(s) discovered, all excluded"
            )

        LOGGER.info(
            "%d case(s) selected across tenant(s) %s: %s",
            len(selection.case_ids),
            ", ".join(selection.tenants),
            ", ".join(str(case_id) for case_id in selection.case_ids),
        )
        return {
            "case_ids": selection.case_ids,
            "members": [m.model_dump() for m in selection.members],
            "excluded": [e.model_dump() for e in selection.excluded],
        }

    selection = select_cases(discover_scope.output)

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
        from radiant.tasks.nextflow.resolve import resolve_families

        families = resolve_families(selected["members"], phenotype_rows)
        LOGGER.info(
            "resolved %d case(s) in tenant(s) %s: %s",
            len(families),
            ", ".join(sorted({f.tenant_code for f in families})),
            ", ".join(f"{f.family_id} ({len(f.members)} member(s), {f.sequencing_type})" for f in families),
        )
        return [f.model_dump() for f in families]

    @task(task_id="list_tenants", task_display_name="[PyOp] List the tenants to register into")
    def list_tenants(families: Any) -> Any:
        """One batch PATCH per tenant, and one mapped `register_tasks` per batch.

        Mapped rather than one task looping: PATCH *appends*, so a single task that failed
        halfway and was retried would double-register every tenant that had already
        succeeded -- a second annotation task per case, pointing at the same outputs and
        indistinguishable from a legitimate re-run. Mapping gives per-tenant retry for free.
        """
        from radiant.tasks.nextflow.model import Family
        from radiant.tasks.nextflow.resolve import tenants_of

        return tenants_of([Family(**f) for f in families])

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
            inputs_subdir=INPUTS_SUBDIR,
            outputs_subdir=OUTPUTS_SUBDIR,
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

    @task(
        task_id="register_tasks",
        task_display_name="[PyOp] Register tasks in the portal",
        max_active_tis_per_dagrun=1,
    )
    def register_tasks(tenant: str, families: Any, collected: Any) -> Any:
        import json

        from airflow.exceptions import AirflowFailException, AirflowNotFoundException
        from airflow.hooks.base import BaseHook
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.batch import build_patch_body
        from radiant.tasks.nextflow.model import Family
        from radiant.tasks.nextflow.portal import fetch_token, patch_case_batch, wait_for_batch

        context = get_current_context()
        dry_run = context["params"]["dry_run"]
        # This tenant's share of the run. The tenant comes off the cases themselves, so a
        # batch can only ever be addressed where its data actually came from.
        parsed = [Family(**f) for f in families if f["tenant_code"] == tenant]
        if not parsed:
            raise AirflowFailException(f"no resolved case belongs to tenant '{tenant}'")

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
        # Returning green here would be the worst outcome available: nothing registered, so
        # the cases stay eligible and the pipeline redoes them tomorrow, and the night after.
        if not batch_id:
            raise AirflowFailException(
                f"the portal accepted the batch for tenant '{tenant}' but reported no batch id, "
                f"so nothing can be confirmed as registered"
            )

        report = wait_for_batch(conn.host, tenant, token, batch_id)
        LOGGER.info("batch report:\n%s", json.dumps(report, indent=2, default=str)[:8000])
        if str(report.get("status", "")).lower() not in ("success", "succeeded", "completed", "done"):
            raise AirflowFailException(f"batch {batch_id} did not succeed: status={report.get('status')}")
        return report

    families = resolve_cases(selection, fetch_phenotypes.output)
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
    register_tasks.partial(families=families, collected=collected).expand(tenant=list_tenants(families))

    selection >> fetch_phenotypes
    paths >> run_pipeline >> collected


nextflow_postprocessing_cases()
