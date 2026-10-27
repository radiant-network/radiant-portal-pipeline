"""Run the Ferlab quality-control-pipeline from Airflow.

A single task launches a Nextflow *driver* pod on qlin-eks; that driver then spawns
one worker pod per pipeline process through Nextflow's own k8s executor. Airflow
sees one task, Kubernetes sees a fan-out. Same shape as
`nextflow_snv_postprocessing.py`, and the same driver image -- only the pipeline asset
and the ConfigMap pair differ.

DRAGEN metrics
--------------
The samples this runs on have already been through DRAGEN, so the report is built
from DRAGEN's per-sample metric CSVs rather than recomputed. That mode is selected per
sample by the samplesheet's ``dragenMetrics`` column (a directory or a tarball), not by
a param here. A row with that column empty silently launches a full BAM_QC/VCF_QC
recompute for its sample, which is hours of worker pods: the cases DAG always fills it,
and a hand-written samplesheet should too.

Run-invariant settings (reference genome, somalier sites, QC thresholds, cohort
mode) live in the `nextflow-qc-params` ConfigMap, the executor and resource layout in
`nextflow-qc-cfg` -- both owned by the kustomization in qlin-qa-infra. Only the two
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
            "sex, status, relationship_to_proband, affected_status, dragenMetrics, lane, runId "
            "optional. dragenMetrics is the sample's directory or tarball of DRAGEN metric CSVs; "
            "setting it is what skips BAM_QC and VCF_QC for that sample."
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
    # Runs do not share state: RUN_TAG keys the launch dir, the work dir and the outdir, so
    # several drivers can run side by side. Five is a budget on cluster load, not a lock.
    max_active_runs=5,
    tags=["radiant", "nextflow", "qc", "manual"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_quality_control.md"),
)
def nextflow_quality_control():
    # The `qc-` prefix is load-bearing, not labelling. RUN_TAG drives the Nextflow
    # workDir, the launch dir *and* the default outdir, and Airflow run ids are only
    # unique within a DAG -- while this DAG and radiant-nextflow-snv-postprocessing are
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
        run_tag=run_tag,
    )
    cleanup = operators.NextflowQualityControl.get_cleanup_work(run_tag=run_tag)

    # Default trigger rule (all_success) on purpose: the scratch is exactly what
    # `-resume` reads, so cleaning up after a failure would make every retry a full
    # re-run. A failed run keeps its workdir until the work-cleanup CronJob ages it out.
    run >> cleanup


nextflow_quality_control()
