"""Annotate somatic calling VCFs with the Ferlab snv-post-processing from Airflow (RAD-23).

The germline pipeline, entered at its `normalize` step with `tools = vep`: split multi-allelic
records, left-align indels, then VEP. Same launch script, image and `nextflow-snv-cfg` as
`radiant-nextflow-snv-postprocessing`, so the same VEP; only the params ConfigMap differs
(`nextflow-snv-somatic-params`, owned by the kustomization in qlin-qa-infra).

A DAG of its own, not a params switch on the germline launcher: that one runs a single driver at a
time, so a somatic run would wait behind hours of germline WGS. Concurrent drivers are safe, the
launch and work dirs being keyed by RUN_TAG.

Started by `radiant-nextflow-somatic-snv-annotation-cases`. Resume and retries work as for the
germline launcher (see its module docstring).
"""

import pendulum
from airflow.decorators import dag
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, NAMESPACE, load_docs_md

# K8s only: the driver needs the FSx-Lustre PVC and the `nextflow` namespace.
from radiant.dags.operators import k8s as operators

dag_params = {
    "input": Param(
        type="string",
        title="Input samplesheet (CSV)",
        description=(
            "Absolute path to the samplesheet on the shared workspace, columns familyId, sequencingType, "
            "vcf, e.g. /workspace/inputs/somatic-snv-runs/<run tag>/samplesheet.csv"
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
    dag_id=f"{NAMESPACE}-nextflow-somatic-snv-annotation",
    dag_display_name="Radiant - Nextflow Somatic SNV Annotation",
    # retries are load-bearing: they are what makes -resume reachable.
    default_args=DEFAULT_ARGS | {"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["radiant", "nextflow", "somatic", "manual"],
    params=dag_params,
    doc_md=load_docs_md("nextflow_somatic_snv_annotation.md"),
)
def nextflow_somatic_snv_annotation():
    # Not ts_nodash: an Airflow 3 manual run can have a null logical_date.
    run_tag = "{{ run_id | replace(':', '-') | replace('+', '-') }}"

    run = operators.NextflowSomaticAnnotation.get_run_annotation(
        input_csv="{{ params.input }}",
        outdir="{{ params.outdir }}",
        run_tag=run_tag,
    )
    cleanup = operators.NextflowSomaticAnnotation.get_cleanup_work(run_tag=run_tag)

    # all_success on purpose: the scratch is what `-resume` reads after a failure.
    run >> cleanup


nextflow_somatic_snv_annotation()
