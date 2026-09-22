"""Run the Ferlab cnv-post-processing pipeline (truvari / mosdepth / VEP / Exomiser / slivar
over germline CNV VCFs) from Airflow.

Sibling of `nextflow_postprocessing.py`, built the same way: a single task launches a
Nextflow *driver* pod on qlin-eks, and that driver spawns one worker pod per pipeline
process through Nextflow's own k8s executor. Airflow sees one task, Kubernetes a fan-out.

Run-invariant settings (reference FASTA, VEP cache, Exomiser data, thresholds) live in the
`nextflow-cnv-params` ConfigMap, the executor and resource layout in `nextflow-cnv-cfg` --
its own pair, because a `-c` config that references a param the `-params-file` does not
declare kills the run at config parse. Only the two values that change every run are
exposed here as DAG params.

Resume
------
The Nextflow launch directory is keyed by ``RUN_TAG``, derived from the Airflow ``run_id``
with a ``cnv-`` prefix: stable across task retries, unique per DAG run, and apart from the
other launchers' runs on the shared filesystem (Airflow run ids are only unique *within* a
DAG). A retry re-enters the same launch dir and `-resume` skips what already completed.
That only helps if the task actually retries, hence the explicit ``retries``.
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
            "/workspace/inputs/cnv-runs/<run tag>/samplesheet.csv"
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
    dag_id=f"{NAMESPACE}-nextflow-cnv-postprocessing",
    dag_display_name="Radiant - Nextflow CNV Post-processing",
    # retries are load-bearing here, not defensive: they are what makes -resume
    # reachable (see the module docstring).
    default_args=DEFAULT_ARGS | {"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    # One driver at a time: the cases DAG fires one run per night, and a second
    # concurrent driver would only compete for the same FSx filesystem.
    max_active_runs=1,
    tags=["radiant", "nextflow", "cnv", "manual"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_cnv_postprocessing.md"),
)
def nextflow_cnv_postprocessing():
    # Not ts_nodash: an Airflow 3 manual run can have a null logical_date, and
    # date-derived templates then raise UndefinedError at render time.
    #
    # `cnv-` keeps this DAG's launch dirs, work dirs and default outdirs apart from
    # radiant-nextflow-postprocessing's and radiant-nextflow-quality-control's on the
    # shared filesystem: sharing a launch dir corrupts the resume cache, and either
    # cleanup_work would delete the other's scratch.
    run_tag = "cnv-{{ run_id | replace(':', '-') | replace('+', '-') }}"

    run = operators.NextflowCnvPostprocessing.get_run_cnv_postprocessing(
        input_csv="{{ params.input }}",
        outdir="{{ params.outdir }}",
        run_tag=run_tag,
    )
    cleanup = operators.NextflowCnvPostprocessing.get_cleanup_work(run_tag=run_tag)

    # Default trigger rule (all_success) on purpose: the scratch is exactly what
    # `-resume` reads, so cleaning up after a failure would make every retry a full
    # re-run. A failed run keeps its workdir until the work-cleanup CronJob ages it out.
    run >> cleanup


nextflow_cnv_postprocessing()
