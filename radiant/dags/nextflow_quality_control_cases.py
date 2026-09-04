"""Find the germline cases waiting for a QC report, run the pipeline, register the result.

`radiant-nextflow-quality-control` runs the Ferlab quality-control-pipeline in DRAGEN-metrics
mode. This DAG closes the two manual ends around it, the way `nextflow_postprocessing_cases`
does for annotation: it discovers which cases have been aligned but never QC'd, builds the
samplesheet, triggers that DAG, then registers what the run published back onto the cases as
one `quality_control_metrics` task each, carrying the per-family MultiQC set as `aggqc`
documents.

    discover_scope -> select_cases -> locate_metrics -> group_cases -> generate_inputs (one per dir)
                                                                              |
                                                                        run_pipeline (one per dir)
                                                                              |
              register_tasks (one per tenant) <- collect_outputs <------------+

Two things make this more than a copy of the annotation DAG:

- the DRAGEN metrics are not documents. `locate_metrics` probes S3 for
  `<aliquot>.mapping_metrics.csv` next to each alignment's output documents, rather than
  guessing a convention, because the pipeline matches metrics by exact sample name and *fails
  open* -- a wrong directory is a green run with an empty report;
- `--dragen_metrics_dir` is one directory per Nextflow run. Cases are grouped by the directory
  the probe found, neighbouring directories are merged into a safe common parent, and one
  launcher run is fired per group; the launcher runs up to five at a time.

Full analysis: `design/SJRA-1879-nextflow-quality-control-automation.md`.
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

# Same workspace as post-processing: the Airflow tasks read and write S3, the pipeline reads
# and writes the FSx mount those buckets are wired to.
INPUTS_ROOT_ENV = "NEXTFLOW_INPUTS_ROOT"
OUTPUTS_ROOT_ENV = "NEXTFLOW_OUTPUTS_ROOT"
DEFAULT_INPUTS_MOUNT = "/workspace/inputs"
DEFAULT_OUTPUTS_MOUNT = "/workspace/outputs"

OUTPUT_POLL_ATTEMPTS = max(1, int(os.getenv("NEXTFLOW_OUTPUT_POLL_ATTEMPTS", "10")))
OUTPUT_POLL_INTERVAL_SECONDS = int(os.getenv("NEXTFLOW_OUTPUT_POLL_INTERVAL", "30"))

# Tenants the service account has `ingest_data` on. Defaults to the post-processing list: it
# is the same service account, and a grant for one registration is a grant for the other.
TENANTS_ENV = "NEXTFLOW_QC_TENANTS"
FALLBACK_TENANTS_ENV = "NEXTFLOW_POSTPROCESSING_TENANTS"

PIPELINE_DAG_ID = f"{NAMESPACE}-nextflow-quality-control"

# Under the shared roots, apart from post-processing (`postprocessing-runs/`, `postprocessing/`).
INPUTS_SUBDIR = "qc-runs"
OUTPUTS_SUBDIR = "qc"

# The launcher accepts this many concurrent runs; each has its own launch dir and outdir.
LAUNCHER_MAX_ACTIVE_RUNS = 5


def _default_tenants() -> list[str]:
    raw = os.getenv(TENANTS_ENV) or os.getenv(FALLBACK_TENANTS_ENV, "")
    return [tenant.strip() for tenant in raw.split(",") if tenant.strip()]


dag_params = {
    "task_ids": Param(
        [],
        type="array",
        items={"type": "integer"},
        title="Alignment task ids",
        description=(
            "`alignment_germline_variant_calling` task ids, for a targeted rerun. Leave empty -- "
            "as a scheduled run does -- to process every case that has been aligned but never "
            "QC'd. A task shared by several cases runs all of them."
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
            f"registration. Defaults to ${TENANTS_ENV}, then ${FALLBACK_TENANTS_ENV}; empty means no filtering."
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


def _group_paths(env: dict[str, str], run_tag: str) -> dict:
    from radiant.tasks.nextflow.paths import run_paths

    return run_paths(
        inputs_root=env["inputs_root"],
        outputs_root=env["outputs_root"],
        inputs_mount=env["inputs_mount"],
        outputs_mount=env["outputs_mount"],
        run_tag=run_tag,
        inputs_subdir=INPUTS_SUBDIR,
        outputs_subdir=OUTPUTS_SUBDIR,
    )


@dag(
    dag_id=f"{NAMESPACE}-nextflow-quality-control-cases",
    dag_display_name="Radiant - Nextflow Quality Control (from Cases)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    # The entire concurrency story: discovery runs at the start of a run, so a queued run
    # re-queries after the previous one has registered and sees the shorter list.
    max_active_runs=1,
    tags=["radiant", "nextflow", "qc"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_quality_control_cases.md"),
    render_template_as_native_obj=True,
    template_searchpath=["/opt/airflow/dags/radiant/dags/sql"],
)
def nextflow_quality_control_cases():
    discover_scope = RadiantStarRocksOperator(
        task_id="discover_scope",
        task_display_name="[StarRocks] Discover cases pending quality control",
        sql="./sql/clinical/pending_quality_control_select.sql",
        parameters={
            "task_ids": "{{ params.task_ids }}",
            "tenants": "{{ params.tenants }}",
        },
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )

    @task(task_id="select_cases", task_display_name="[PyOp] Select the cases that can run")
    def select_cases(document_rows: Any) -> Any:
        """Fold the document rows into members, keep what can run, report what cannot."""
        from airflow.exceptions import AirflowSkipException
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.qc.resolve import select_cases as select

        context = get_current_context()
        requested = list(context["params"]["task_ids"] or [])
        selection = select(document_rows, requested, strict=bool(requested))

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

    @task(task_id="locate_metrics", task_display_name="[PyOp] Locate each case's DRAGEN metrics on S3")
    def locate_metrics(selected: Any) -> Any:
        """Probe S3 for each member's metrics and drop the cases that have none."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.qc.metrics import S3Lister
        from radiant.tasks.nextflow.qc.metrics import locate_metrics as locate
        from radiant.tasks.nextflow.qc.resolve import resolve_cases

        context = get_current_context()
        env = _workspace_env()
        cases = resolve_cases(selected["members"])
        lister = S3Lister()
        kept, excluded = locate(cases, lister.list_dir, lister.list_tree, env["inputs_root"])

        # Named tasks are an operator's request: a case whose metrics cannot be found is an
        # error they must see. A discovered scope is the job's own doing: a line in a report.
        if excluded and context["params"]["task_ids"]:
            raise AirflowFailException("\n".join(f"{e.reason}: {e.detail}" for e in excluded))
        if not kept:
            raise AirflowSkipException(f"nothing to run: {len(excluded)} case(s) had no locatable DRAGEN metrics")
        return [c.model_dump() for c in kept]

    @task(task_id="group_cases", task_display_name="[PyOp] Group cases by metrics directory")
    def group_cases(cases: Any) -> Any:
        """One launcher run if every case's metrics share a safe common directory, otherwise one
        per directory. A plain list, because Airflow can only map over a task's return value,
        not a keyed XCom."""
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.paths import sanitize_run_tag
        from radiant.tasks.nextflow.qc.metrics import S3Lister, group_by_dir
        from radiant.tasks.nextflow.qc.model import QcCase

        env = _workspace_env()
        groups = group_by_dir(
            [QcCase(**c) for c in cases],
            sanitize_run_tag(get_current_context()["run_id"]),
            S3Lister().list_tree,
            env["inputs_root"],
        )
        for group in groups:
            LOGGER.info("run %s: %s -> case(s) %s", group.run_tag, group.metrics_dir_s3, group.case_ids)
        return [g.model_dump() for g in groups]

    @task(task_id="generate_inputs", task_display_name="[PyOp] Generate one samplesheet per metrics directory")
    def generate_inputs(group: Any, cases: Any) -> Any:
        """Write one group's samplesheet to its own S3 prefix, and return the child run's
        `trigger_run_id` and `conf` -- the shape `expand_kwargs` needs downstream."""
        import boto3

        from radiant.tasks.nextflow.paths import split_s3_uri, to_mount
        from radiant.tasks.nextflow.qc.inputs import build_inputs
        from radiant.tasks.nextflow.qc.model import MetricsGroup, QcCase

        env = _workspace_env()
        parsed_group = MetricsGroup(**group)
        parsed_cases = [QcCase(**c) for c in cases if c["case_id"] in parsed_group.case_ids]
        paths = _group_paths(env, parsed_group.run_tag)
        files = build_inputs(parsed_cases, env["inputs_root"], env["inputs_mount"])

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

        return {
            # Pinned, not auto-generated: a retry re-enters the same child run and its
            # Nextflow launch dir, so `-resume` finds what already completed.
            "trigger_run_id": parsed_group.run_tag,
            "conf": {
                "input": paths["samplesheet_pod"],
                "dragen_metrics_dir": to_mount(parsed_group.metrics_dir_s3, env["inputs_root"], env["inputs_mount"]),
                "outdir": paths["outdir_pod"],
            },
        }

    @task(task_id="collect_outputs", task_display_name="[PyOp] Collect pipeline outputs")
    def collect_outputs(cases: Any, groups: Any) -> Any:
        import time

        import boto3

        from radiant.tasks.nextflow.paths import split_s3_uri
        from radiant.tasks.nextflow.qc.model import MetricsGroup, QcCase
        from radiant.tasks.nextflow.qc.outputs import MissingOutputsError, collect

        env = _workspace_env()
        parsed_cases = [QcCase(**c) for c in cases]
        s3 = boto3.client("s3")
        collected: dict[str, dict] = {}

        for raw in groups:
            group = MetricsGroup(**raw)
            group_cases = [c for c in parsed_cases if c.case_id in group.case_ids]
            outdir_s3 = _group_paths(env, group.run_tag)["outdir_s3"]
            bucket, prefix = split_s3_uri(outdir_s3)
            last_error = None
            # The FSx export is a best-effort drain, not a barrier, so the outputs can lag
            # the driver pod's exit by a little.
            for attempt in range(1, OUTPUT_POLL_ATTEMPTS + 1):
                listing = {
                    obj["Key"][len(prefix) + 1 :]: obj["Size"]
                    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
                    for obj in page.get("Contents", [])
                }
                try:
                    collected.update(collect(group_cases, listing, outdir_s3))
                    last_error = None
                    break
                except MissingOutputsError as error:
                    last_error = error
                    LOGGER.warning("%s attempt %d/%d: %s", group.run_tag, attempt, OUTPUT_POLL_ATTEMPTS, error)
                    if attempt < OUTPUT_POLL_ATTEMPTS:
                        time.sleep(OUTPUT_POLL_INTERVAL_SECONDS)
            if last_error is not None:
                raise last_error

        LOGGER.info("collected outputs for %d case(s)", len(collected))
        return collected

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
        from airflow.operators.python import get_current_context

        from radiant.tasks.nextflow.qc.batch import build_patch_body
        from radiant.tasks.nextflow.qc.model import QcCase
        from radiant.tasks.nextflow.register import register_case_batch

        context = get_current_context()
        parsed = [QcCase(**c) for c in cases if c["tenant_code"] == tenant]
        if not parsed:
            raise AirflowFailException(f"no resolved case belongs to tenant '{tenant}'")
        body = build_patch_body(parsed, collected)
        return register_case_batch(tenant, body, context["params"]["dry_run"])

    selection = select_cases(discover_scope.output)
    cases = locate_metrics(selection)
    groups = group_cases(cases)

    inputs = generate_inputs.partial(cases=cases).expand(group=groups)

    run_pipeline = TriggerDagRunOperator.partial(
        task_id="run_pipeline",
        task_display_name="[DAG] Run Nextflow Quality Control",
        trigger_dag_id=PIPELINE_DAG_ID,
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=True,
        poke_interval=60,
        # Matches the launcher's own limit, so the children queue here rather than there.
        max_active_tis_per_dagrun=LAUNCHER_MAX_ACTIVE_RUNS,
    ).expand_kwargs(inputs)

    collected = collect_outputs(cases, groups)
    register_tasks.partial(cases=cases, collected=collected).expand(tenant=list_tenants(cases))

    run_pipeline >> collected


nextflow_quality_control_cases()
