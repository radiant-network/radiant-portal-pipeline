"""Run the Ferlab snv-post-processing (VEP / slivar / Exomiser) from Airflow.

A single task launches a Nextflow *driver* pod on qlin-eks; that driver then spawns
one worker pod per pipeline process through Nextflow's own k8s executor. Airflow
sees one task, Kubernetes sees a fan-out.

Run-invariant settings (reference paths, tools, VEP/Exomiser versions) live in a params
ConfigMap, the executor and resource layout in `nextflow-snv-cfg` -- both owned by the
kustomization in qlin-qa-infra. Only the two values that change every run are exposed here
as DAG params, plus `params_set`, which picks the params ConfigMap:

- `germline` (`nextflow-snv-params`): joint genotyping from gVCFs, then VEP, slivar, Exomiser;
- `somatic` (`nextflow-snv-somatic-params`): `step = normalize`, `tools = vep`, over the raw
  VCF of a somatic calling task (RAD-23). Same pipeline revision and config, so same VEP.

Concurrency
-----------
Two drivers may run at once (`max_active_runs=2`), so the somatic run never waits behind
hours of germline WGS, and the scheduled import behind both. They cannot collide: the launch
dir, the work dir (`workDir = /workspace/work/${runTag}` in `nextflow-snv-cfg`), the outdir
and the cleanup are all keyed by RUN_TAG, and the somatic cases DAG prefixes its tag with
`somatic-`. Germline runs stay one at a time through the germline cases DAG's own
`max_active_runs=1`; two manual germline WGS runs at once would need ~2x the scratch space.

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
    "params_set": Param(
        default="germline",
        type="string",
        enum=sorted(operators.NextflowPostprocessing.params_configmaps()),
        title="Params set",
        description=(
            "Which params ConfigMap the run reads. `germline`: joint genotyping from gVCFs, then VEP, "
            "slivar and Exomiser. `somatic`: split multi-allelics then VEP over a calling task's raw VCF."
        ),
    ),
}

# Rendered by the operator (`V1ConfigMapVolumeSource.name` is a templated field), so the choice
# is the run's. An if/else rather than a dict literal: the names come from env vars and are
# never pasted into Jinja syntax.
_PARAMS_CONFIGMAPS = operators.NextflowPostprocessing.params_configmaps()
_PARAMS_CONFIGMAP = (
    "{% if params.params_set == 'somatic' %}"
    + _PARAMS_CONFIGMAPS["somatic"]
    + "{% else %}"
    + _PARAMS_CONFIGMAPS["germline"]
    + "{% endif %}"
)


@dag(
    dag_id=f"{NAMESPACE}-nextflow-snv-postprocessing",
    dag_display_name="Radiant - Nextflow SNV Post-processing",
    # retries are load-bearing here, not defensive: they are what makes -resume
    # reachable (see the module docstring).
    default_args=DEFAULT_ARGS | {"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    # Two drivers at once, one germline and one somatic: see "Concurrency" in the module
    # docstring. Everything a run writes is keyed by RUN_TAG, so they cannot collide.
    max_active_runs=2,
    tags=["radiant", "nextflow", "manual"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_snv_postprocessing.md"),
)
def nextflow_snv_postprocessing():
    # Not ts_nodash: an Airflow 3 manual run can have a null logical_date, and
    # date-derived templates then raise UndefinedError at render time.
    run_tag = "{{ run_id | replace(':', '-') | replace('+', '-') }}"

    run = operators.NextflowPostprocessing.get_run_postprocessing(
        input_csv="{{ params.input }}",
        outdir="{{ params.outdir }}",
        run_tag=run_tag,
        params_configmap=_PARAMS_CONFIGMAP,
    )
    cleanup = operators.NextflowPostprocessing.get_cleanup_work(run_tag=run_tag)

    # Default trigger rule (all_success) on purpose: the scratch is exactly what
    # `-resume` reads, so cleaning up after a failure would make every retry a full
    # re-run. A failed run keeps its workdir until the work-cleanup CronJob ages it out.
    run >> cleanup


nextflow_snv_postprocessing()
