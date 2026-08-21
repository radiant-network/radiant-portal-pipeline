"""Run the Ferlab Post-processing-Pipeline (VEP / slivar / Exomiser) from Airflow.

A single task launches a Nextflow *driver* pod on qlin-eks; that driver then spawns
one worker pod per pipeline process through Nextflow's own k8s executor. Airflow
sees one task, Kubernetes sees a fan-out.

Run-invariant settings (reference paths, tools, VEP/Exomiser versions) live in the
`nextflow-params` ConfigMap, the executor and resource layout in `nextflow-cfg` --
both owned by the kustomization in qlin-qa-infra. Only the two values that change
every run are exposed here as DAG params.

Resume
------
The Nextflow launch directory is keyed by ``RUN_TAG``, derived from the Airflow
``run_id``: stable across task retries, unique per DAG run. A retry therefore
re-enters the same launch dir and `-resume` skips what already completed, which on
WGS is the difference between minutes and hours. It only helps if the task actually
retries, hence the explicit ``retries`` below -- ``DEFAULT_ARGS`` carries only
``owner`` and Airflow defaults ``retries`` to 0.
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
            "/workspace/inputs/1000genomes-dragen-v4-4-7/samplesheet.csv"
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
    dag_id=f"{NAMESPACE}-nextflow-postprocessing",
    dag_display_name="Radiant - Nextflow Post-processing",
    # retries are load-bearing here, not defensive: they are what makes -resume
    # reachable (see the module docstring).
    default_args=DEFAULT_ARGS | {"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    # One driver at a time: concurrent runs would share the same FSx workspace.
    max_active_runs=1,
    tags=["radiant", "nextflow", "manual"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_postprocessing.md"),
)
def nextflow_postprocessing():
    # Not ts_nodash: an Airflow 3 manual run can have a null logical_date, and
    # date-derived templates then raise UndefinedError at render time.
    run_tag = "{{ run_id | replace(':', '-') | replace('+', '-') }}"

    run = operators.NextflowPostprocessing.get_run_postprocessing(
        input_csv="{{ params.input }}",
        outdir="{{ params.outdir }}",
        run_tag=run_tag,
    )
    cleanup = operators.NextflowPostprocessing.get_cleanup_work(run_tag=run_tag)

    # Default trigger rule (all_success) on purpose: the scratch is exactly what
    # `-resume` reads, so cleaning up after a failure would make every retry a full
    # re-run. A failed run keeps its workdir until the work-cleanup CronJob ages it out.
    run >> cleanup


nextflow_postprocessing()
