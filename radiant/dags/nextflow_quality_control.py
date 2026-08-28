"""Run the Ferlab quality-control-pipeline from Airflow, in DRAGEN-metrics mode.

A single task launches a Nextflow *driver* pod on qlin-eks; that driver then spawns
one worker pod per pipeline process through Nextflow's own k8s executor. Airflow
sees one task, Kubernetes sees a fan-out. Same shape as
`nextflow_postprocessing.py`, and the same driver image -- only the pipeline asset
and the ConfigMap pair differ.

DRAGEN metrics only
-------------------
The samples this runs on have already been through DRAGEN, so the report is built
from DRAGEN's per-sample metric CSVs rather than recomputed. That mode is selected
purely by passing ``--dragen_metrics_dir``, which is why the param is **required**
here although it is optional upstream: triggering without it would silently launch a
full BAM_QC/VCF_QC recompute, which is hours of worker pods nobody asked for.

Run-invariant settings (reference genome, somalier sites, QC thresholds, cohort
mode) live in the `nextflow-qc-params` ConfigMap, the executor and resource layout in
`nextflow-qc-cfg` -- both owned by the kustomization in qlin-qa-infra. Only the three
values that change every run are exposed here as DAG params.

Resume
------
The Nextflow launch directory is keyed by ``RUN_TAG``, derived from the Airflow
``run_id``: stable across task retries, unique per DAG run. A retry therefore
re-enters the same launch dir and `-resume` skips what already completed. It only
helps if the task actually retries, hence the explicit ``retries`` below --
``DEFAULT_ARGS`` carries only ``owner`` and Airflow defaults ``retries`` to 0.
"""

import pendulum
from airflow.decorators import dag
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md

# K8s only: the driver needs the FSx-Lustre PVC and the `nextflow` namespace, so
# unlike import_part.py this DAG has no ECS branch.
from radiant.dags.operators import k8s as operators

dag_params = {
    "input": Param(
        type="string",
        title="Input samplesheet (CSV)",
        description=(
            "Absolute path to the samplesheet on the shared workspace, e.g. "
            "/workspace/inputs/qc/1000genomes-dragen-v4-4-7/samplesheet.csv. Columns: "
            "participant,sample,fileType,file1 required; file2, familyId, experimentalStrategy, "
            "sex, status, relationship_to_proband, affected_status, lane, runId optional."
        ),
        minLength=1,
    ),
    "dragen_metrics_dir": Param(
        type="string",
        title="DRAGEN metrics directory",
        description=(
            "Absolute path to the directory of DRAGEN per-sample metric CSVs. Globbed at any "
            "depth and matched to samplesheet rows by sample prefix; files matching no row are "
            "silently ignored. Setting it is what skips BAM_QC and VCF_QC."
        ),
        minLength=1,
    ),
    "outdir": Param(
        default="",
        type="string",
        title="Output directory",
        description=("Absolute path for published outputs. Leave empty to use /workspace/outputs/qlin/<run tag>."),
    ),
}


@dag(
    dag_id=f"{NAMESPACE}-nextflow-quality-control",
    dag_display_name="Radiant - Nextflow Quality Control",
    # retries are load-bearing here, not defensive: they are what makes -resume
    # reachable (see the module docstring).
    default_args=DEFAULT_ARGS | {"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    # One driver at a time: concurrent runs would share the FSx workspace.
    max_active_runs=1,
    tags=["radiant", "nextflow", "qc", "manual"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_quality_control.md"),
)
def nextflow_quality_control():
    # The `qc-` prefix is load-bearing, not labelling. RUN_TAG drives the Nextflow
    # workDir, the launch dir *and* the default outdir, and Airflow run ids are only
    # unique within a DAG -- while this DAG and radiant-nextflow-postprocessing are
    # separate DAGs that can run at the same time (and the cases DAG pins a child
    # run id explicitly). Without the prefix, two runs could share one launch dir,
    # corrupting the resume cache, and either cleanup could delete the other's scratch.
    #
    # Not ts_nodash: an Airflow 3 manual run can have a null logical_date, and
    # date-derived templates then raise UndefinedError at render time.
    run_tag = "qc-{{ run_id | replace(':', '-') | replace('+', '-') }}"

    run = operators.NextflowQualityControl.get_run_quality_control(
        input_csv="{{ params.input }}",
        outdir="{{ params.outdir }}",
        dragen_metrics_dir="{{ params.dragen_metrics_dir }}",
        run_tag=run_tag,
    )
    cleanup = operators.NextflowQualityControl.get_cleanup_work(run_tag=run_tag)

    # Default trigger rule (all_success) on purpose: the scratch is exactly what
    # `-resume` reads, so cleaning up after a failure would make every retry a full
    # re-run. A failed run keeps its workdir until the work-cleanup CronJob ages it out.
    run >> cleanup


nextflow_quality_control()
