import os

from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s


def _container_resources(profile: str, cpu: str, memory: str, memory_limit: str) -> k8s.V1ResourceRequirements:
    """Build the pod resources for a Radiant task.

    Sizing a task at all is what makes it visible to the scheduler: with no request,
    Karpenter sizes the node as if the pod were empty and lands it on the smallest
    instance in the pool (a 3Gi-allocatable c6a.large in QA), where a heavy task is
    OOMKilled with no memory limit to attribute the kill to.

    Only memory is limited. A memory limit makes an over-budget task fail fast and
    attributably instead of taking down whatever else shares the node, while a CPU
    limit would merely throttle work we want to run at full speed (cyvcf2 reads with
    ``vcf_threads=4`` and pyiceberg writes its parquet files in parallel).

    Each profile is overridable without a deploy via
    ``RADIANT_TASK_OPERATOR_<PROFILE>_{CPU,MEMORY,MEMORY_LIMIT}``.
    """
    return k8s.V1ResourceRequirements(
        requests={
            "cpu": os.getenv(f"RADIANT_TASK_OPERATOR_{profile}_CPU", cpu),
            "memory": os.getenv(f"RADIANT_TASK_OPERATOR_{profile}_MEMORY", memory),
        },
        limits={"memory": os.getenv(f"RADIANT_TASK_OPERATOR_{profile}_MEMORY_LIMIT", memory_limit)},
    )


def _snv_container_resources() -> k8s.V1ResourceRequirements:
    """SNV extraction keeps three TableAccumulator buffers alive at once (occurrence,
    variant, consequence), each growing to PARQUET_FILE_SIZE_MB before it flushes, plus
    a transient copy of the buffer on every flush -- so a trio WGS VCF needs several GB.

    That floor holds however small the VCF is, so a constrained environment can lower it
    with ``RADIANT_PARQUET_FILE_SIZE_MB`` instead of raising the memory here.
    """
    return _container_resources("SNV", cpu="1", memory="4Gi", memory_limit="6Gi")


def _cnv_container_resources() -> k8s.V1ResourceRequirements:
    """CNV emits far fewer records per sample than SNV, so it needs a fraction of the
    memory. The limit leaves headroom because the CNV occurrence buffer is a plain list
    with no flush threshold, materialised in one `pa.Table.from_pylist` at the end
    (`radiant/tasks/vcf/cnv/germline/process.py`).
    """
    return _container_resources("CNV", cpu="1", memory="500Mi", memory_limit="1Gi")


def _metadata_container_resources() -> k8s.V1ResourceRequirements:
    """Committing partitions never touches row data: the partition filters align with the
    tables' partition specs, so the Iceberg delete drops whole files as metadata rather
    than rewriting them, and `add_files` reads one parquet footer at a time.

    Peak therefore scales with the number of files, not the size of the VCFs -- and the
    fan-in is what makes it non-trivial: this task is not mapped, so a single container
    commits every file produced by every mapped germline *and* somatic extraction task in
    the part. Half a core is enough because the footer reads are sequential and
    network-bound.
    """
    return _container_resources("METADATA", cpu="500m", memory="1Gi", memory_limit="2Gi")


def _nextflow_container_resources() -> k8s.V1ResourceRequirements:
    """The Nextflow driver orchestrates; it does not process data. Every VCF actually
    gets read in a worker pod that Nextflow's k8s executor spawns itself, sized by
    `nextflow.config` rather than by anything here.

    So this sizes a JVM that holds the task graph and polls the API server. It is the
    one long-lived pod in the run, which is why it is worth requesting properly: a
    driver OOMKilled halfway through loses the whole pipeline, not one task.
    """
    return _container_resources("NEXTFLOW", cpu="1", memory="2Gi", memory_limit="4Gi")


class RadiantTaskK8SOperator:
    @staticmethod
    def _get_k8s_context(radiant_namespace: str, container_resources: k8s.V1ResourceRequirements | None = None):
        iceberg_env_vars = {
            key: value for key, value in os.environ.items() if key.startswith("PYICEBERG_CATALOG__DEFAULT__")
        }
        # Sizing is passed in rather than set by the caller alongside `task_id` and
        # friends: callers merge as `dict(...) | _get_k8s_context(...)`, so this context
        # wins on key collisions and would silently override a per-task value.
        resources = {"container_resources": container_resources} if container_resources else {}
        return dict(
            namespace=os.getenv("RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE"),
            image=os.getenv("RADIANT_TASK_OPERATOR_IMAGE"),
            service_account_name=os.getenv("RADIANT_TASK_OPERATOR_SERVICE_ACCOUNT_NAME"),
            image_pull_policy="IfNotPresent",
            get_logs=True,
            is_delete_operator_pod=True,
            env_vars={
                "AWS_REGION": os.getenv("AWS_REGION"),
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL"),
                "AWS_ALLOW_HTTP": os.getenv("AWS_ALLOW_HTTP"),
                "RADIANT_ICEBERG_NAMESPACE": radiant_namespace,
                "PYTHONPATH": os.getenv("RADIANT_TASK_OPERATOR_PYTHONPATH"),
                "LD_LIBRARY_PATH": os.getenv("RADIANT_TASK_OPERATOR_LD_LIBRARY_PATH"),
                "RADIANT_PARQUET_FILE_SIZE_MB": os.getenv("RADIANT_PARQUET_FILE_SIZE_MB"),
            }
            | iceberg_env_vars,
            **resources,
        )


class ImportSNVVCF(RadiantTaskK8SOperator):
    @staticmethod
    def get_create_germline_parquet_files(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                pool="import_vcf",
                task_id="create_germline_parquet_files_k8s",
                task_display_name="[K8s] Create Germline Parquet Files",
                map_index_template="Task: {{ task.op_kwargs['radiant_task']['task_id'] }}",
                name="import-germline-vcf-for-task",
                do_xcom_push=True,
            )
            | ImportSNVVCF._get_k8s_context(radiant_namespace, container_resources=_snv_container_resources())
        )
        def k8s_create_germline_parquet_files(
            radiant_task: dict,
        ):  # `task` is a reserved Airflow keyword, so we use `radiant_task`
            import os

            from radiant.tasks.vcf.snv.germline.process import create_parquet_files

            namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE")
            return create_parquet_files(task=radiant_task, namespace=namespace)

        return k8s_create_germline_parquet_files

    @staticmethod
    def get_create_somatic_parquet_files(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                pool="import_vcf",
                task_id="create_somatic_parquet_files_k8s",
                task_display_name="[K8s] Create Somatic Parquet Files",
                map_index_template="Task: {{ task.op_kwargs['radiant_task']['task_id'] }}",
                name="import-somatic-vcf-for-task",
                do_xcom_push=True,
            )
            | ImportSNVVCF._get_k8s_context(radiant_namespace, container_resources=_snv_container_resources())
        )
        def k8s_create_somatic_parquet_files(
            radiant_task: dict,
        ):  # `task` is a reserved Airflow keyword, so we use `radiant_task`
            import os

            from radiant.tasks.vcf.snv.somatic.process import create_parquet_files

            namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE")
            return create_parquet_files(task=radiant_task, namespace=namespace)

        return k8s_create_somatic_parquet_files

    @staticmethod
    def get_commit_partitions(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="commit_partitions_k8s",
                task_display_name="[K8s] Commit Partitions",
                name="commit-partitions",
                do_xcom_push=True,
            )
            | ImportSNVVCF._get_k8s_context(radiant_namespace, container_resources=_metadata_container_resources()),
        )
        def k8s_commit_partitions(table_partitions: dict[str, list[dict]]):
            from radiant.tasks.iceberg.utils import commit_partitions

            commit_partitions(table_partitions)

        return k8s_commit_partitions


class ImportPart(RadiantTaskK8SOperator):
    @staticmethod
    def get_import_cnv_vcf(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="import_cnv_vcf_k8s",
                task_display_name="[K8s] Import CNV VCF",
                name="import-cnv-vcf",
                do_xcom_push=True,
            )
            | ImportPart._get_k8s_context(radiant_namespace, container_resources=_cnv_container_resources())
        )
        def import_cnv_vcf(tasks: list[dict]) -> None:
            import os

            from radiant.tasks.vcf.cnv.germline.process import import_cnv_vcf as _import_cnv_vcf

            namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE")
            _import_cnv_vcf(tasks=tasks, namespace=namespace)

        return import_cnv_vcf

    @staticmethod
    def get_import_somatic_cnv_vcf(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="import_somatic_cnv_vcf_k8s",
                task_display_name="[K8s] Import Somatic CNV VCF",
                name="import-somatic-cnv-vcf",
                do_xcom_push=True,
            )
            # Same profile as germline CNV: a tumor-only CNV file is single-sample and segment-level
            # (~304 segments for a WES sample), so it is no heavier than the germline one.
            | ImportPart._get_k8s_context(radiant_namespace, container_resources=_cnv_container_resources())
        )
        def import_somatic_cnv_vcf(tasks: list[dict]) -> None:
            import os

            from radiant.tasks.vcf.cnv.somatic.process import import_somatic_cnv_vcf as _import_somatic_cnv_vcf

            namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE")
            _import_somatic_cnv_vcf(tasks=tasks, namespace=namespace)

        return import_somatic_cnv_vcf


class InitIcebergTables(RadiantTaskK8SOperator):
    @staticmethod
    def get_init_database(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="init_database_k8s", task_display_name="[K8s] Init Data", name="init-data", do_xcom_push=True
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def init_database():
            from radiant.tasks.iceberg import initialization

            initialization.init_database()

        return init_database

    @staticmethod
    def get_create_germline_snv_occurrence_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_germline_snv_occurrence_table_k8s",
                task_display_name="[K8s] Create Germline SNV Occurrence Table",
                name="create-germline-snv-occurrence-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_germline_snv_occurrence_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_germline_snv_occurrence_table()

        return create_germline_snv_occurrence_table

    @staticmethod
    def get_create_variant_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_variant_table_k8s",
                task_display_name="[K8s] Create SNV Variants Table",
                name="create-snv-variants-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_variant_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_variant_table()

        return create_variant_table

    @staticmethod
    def get_create_consequence_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_consequence_table_k8s",
                task_display_name="[K8s] Create SNV Consequences Table",
                name="create-snv-consequences-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_consequences_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_consequences_table()

        return create_consequences_table

    @staticmethod
    def get_create_germline_cnv_occurrence_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_germline_cnv_occurrence_table_k8s",
                task_display_name="[K8s] Create Germline CNV Occurrences Table",
                name="create-germline-snv-occurrences-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_germline_cnv_occurrence_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_germline_cnv_occurrence_table()

        return create_germline_cnv_occurrence_table

    @staticmethod
    def get_create_somatic_snv_occurrence_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_somatic_snv_occurrence_table_k8s",
                task_display_name="[K8s] Create Somatic SNV Occurrences Table",
                name="create-somatic-snv-occurrences-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_somatic_snv_occurrence_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_somatic_snv_occurrence_table()

        return create_somatic_snv_occurrence_table

    @staticmethod
    def get_create_somatic_cnv_occurrence_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_somatic_cnv_occurrence_table_k8s",
                task_display_name="[K8s] Create Somatic CNV Occurrences Table",
                name="create-somatic-cnv-occurrences-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_somatic_cnv_occurrence_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_somatic_cnv_occurrence_table()

        return create_somatic_cnv_occurrence_table


class CheckDataIntegrity:
    """dbt data-quality run. Reuses the shared Radiant K8s deployment settings
    (namespace, service account) but overrides the image and launches it
    directly via KubernetesPodOperator."""

    @staticmethod
    def get_run_dbt(run_results_s3_uri: str, junit_s3_uri: str, tenants: str) -> KubernetesPodOperator:
        return KubernetesPodOperator(
            task_id="run_dbt",
            task_display_name="[K8s] Run dbt data tests",
            name="data-integrity-dbt",
            namespace=os.getenv("RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE"),
            service_account_name=os.getenv("RADIANT_TASK_OPERATOR_SERVICE_ACCOUNT_NAME"),
            image=os.getenv("RADIANT_DBT_OPERATOR_IMAGE"),
            image_pull_policy="IfNotPresent",
            secrets=[
                Secret("env", "AIRFLOW_CONN_STARROCKS_CONN", "starrocks-airflow-conn", "AIRFLOW_CONN_STARROCKS_CONN"),
            ],
            env_vars={
                "RUN_RESULTS_S3_URI": run_results_s3_uri,
                "JUNIT_S3_URI": junit_s3_uri,
                # JSON list of {"code", "schema"}: one extra dbt pass per entry.
                "TENANTS": tenants,
                # We inject these for the local sandbox (like the other k8s operators).
                # In prod (EKS), boto3 uses Pod Identity instead, so these resolve to empty.
                "AWS_REGION": os.getenv("AWS_REGION"),
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL"),
                "AWS_ALLOW_HTTP": os.getenv("AWS_ALLOW_HTTP"),
            },
            get_logs=True,
            # We only use this operator with the KubernetesExecutor, which runs
            # this task in its own ephemeral pod. Using non-deferrable does not
            # block other tasks.
            deferrable=False,
            is_delete_operator_pod=True,
        )


# `cmds`/`arguments` are Jinja-templated fields, so nothing in these scripts may
# contain `{{` or `{%`; per-run values arrive as env vars instead.
#
# The `cd` is load-bearing: Nextflow reads `.nextflow/cache` from the working
# directory, so a launch dir keyed by RUN_TAG is what lets a retry `-resume`.
#
# Both scripts read their config from the same paths (`/etc/nextflow`,
# `/etc/nextflow-params`) even though the two pipelines need different content --
# it is the ConfigMap *names* that differ, not the mounts, which is what keeps the
# scripts near-identical.
_NEXTFLOW_DRIVER_SCRIPT = """
set -euo pipefail
LAUNCH="${NXF_WORKSPACE}/work/.nextflow-launchdir/${RUN_TAG}"
OUTDIR="${NXF_OUTDIR:-${NXF_WORKSPACE}/outputs/qlin/${RUN_TAG}}"
mkdir -p "$LAUNCH" && cd "$LAUNCH"
echo ">> run_tag=$RUN_TAG input=$NXF_INPUT outdir=$OUTDIR"
nextflow run "${NXF_HOME}/assets/Ferlab-Ste-Justine/Post-processing-Pipeline" \
    -profile docker \
    -c /etc/nextflow/nextflow.config \
    -resume \
    -params-file /etc/nextflow-params/params.json \
    --input "$NXF_INPUT" \
    --outdir "$OUTDIR"
# Let the /outputs DRA flush the last file events to S3 before the pod terminates.
sleep "${POST_RUN_DRAIN_SECONDS}"
"""


# `--dragen_metrics_dir` is what selects the pipeline's DRAGEN-metrics mode: with it
# set, BAM_QC and VCF_QC are skipped and the report is built from DRAGEN's per-sample
# CSVs. It is unconditional here because that is the only mode this DAG runs -- the
# param is required, so the variable is never empty.
#
# The pipeline is run from a copy on the SHARED filesystem, not from the image's own
# `${NXF_HOME}/assets/...`, and that is load-bearing rather than tidiness.
#
# This pipeline carries nf-core module resource scripts (`modules/local/multiqc_python/
# resources/usr/bin/multiqc_report.py`). Nextflow puts those on PATH by exporting the
# *projectDir* path into the task wrapper -- but the task runs in the module's own
# container (biocontainers/multiqc), which mounts only ${NXF_WORKSPACE} and has no
# /opt/nextflow. Run from the image path, MULTIQC_PYTHON dies with
# `multiqc_report.py: command not found` (exit 127). Post-processing never hit this
# because it has no module resource scripts.
#
# The copy is keyed by the asset's git commit, so a rebuilt image with a new pipeline
# revision lands in a new directory instead of silently reusing a stale one. The
# temp-dir-then-`mv -T` dance makes a concurrent driver racing on the same revision
# harmless: whoever loses the rename just reuses the winner's directory.
#
# No `-r` on `nextflow run`: the source is a local directory whose revision is
# whatever Dockerfile.nextflow.launcher pulled at build time.
_NEXTFLOW_QC_DRIVER_SCRIPT = """
set -euo pipefail
SRC="${NXF_HOME}/assets/Ferlab-Ste-Justine/quality-control-pipeline"
REV="$(git -C "$SRC" rev-parse --short HEAD)"
PROJECT="${NXF_WORKSPACE}/pipelines/quality-control-pipeline-${REV}"
if [ ! -d "$PROJECT" ]; then
  mkdir -p "${NXF_WORKSPACE}/pipelines"
  TMP="${PROJECT}.tmp.$$"
  rm -rf "$TMP"
  cp -a "$SRC" "$TMP"
  mv -T "$TMP" "$PROJECT" 2>/dev/null || rm -rf "$TMP"
fi
echo ">> pipeline rev=$REV project=$PROJECT"
LAUNCH="${NXF_WORKSPACE}/work/.nextflow-launchdir/${RUN_TAG}"
OUTDIR="${NXF_OUTDIR:-${NXF_WORKSPACE}/outputs/qlin/${RUN_TAG}}"
mkdir -p "$LAUNCH" && cd "$LAUNCH"
echo ">> run_tag=$RUN_TAG input=$NXF_INPUT outdir=$OUTDIR dragen=$NXF_DRAGEN_METRICS_DIR"
nextflow run "$PROJECT" \
    -profile docker \
    -c /etc/nextflow/nextflow.config \
    -resume \
    -params-file /etc/nextflow-params/params.json \
    --input "$NXF_INPUT" \
    --outdir "$OUTDIR" \
    --dragen_metrics_dir "$NXF_DRAGEN_METRICS_DIR"
# Let the /outputs DRA flush the last file events to S3 before the pod terminates.
sleep "${POST_RUN_DRAIN_SECONDS}"
"""


# `${RUN_TAG:?}` is load-bearing, not defensive style: an empty RUN_TAG would expand
# the paths below to `/workspace/work/` and `/workspace/work/.nextflow-launchdir/`,
# deleting every run's scratch on the shared filesystem. The guard aborts instead.
_NEXTFLOW_CLEANUP_SCRIPT = """
set -euo pipefail
: "${RUN_TAG:?RUN_TAG is empty - refusing to delete}"
: "${NXF_WORKSPACE:?NXF_WORKSPACE is empty - refusing to delete}"
WORK="${NXF_WORKSPACE}/work/${RUN_TAG}"
LAUNCH="${NXF_WORKSPACE}/work/.nextflow-launchdir/${RUN_TAG}"
echo ">> before:"; df -h "${NXF_WORKSPACE}" | tail -1
for d in "$WORK" "$LAUNCH"; do
  if [ -d "$d" ]; then
    echo ">> removing $d ($(du -sh "$d" 2>/dev/null | cut -f1))"
    rm -rf "$d"
  else
    echo ">> $d absent, nothing to do"
  fi
done
echo ">> after:"; df -h "${NXF_WORKSPACE}" | tail -1
"""


def _nextflow_image(*env_vars: str) -> str | None:
    """First env var that is set wins.

    Lets one pipeline be pointed at an ad-hoc image -- a locally built tag while a
    revision is being tested -- without moving the other one with it. Returns None
    when none is set, so a missing image fails loudly at pod creation rather than
    silently pulling something unintended.
    """
    for name in env_vars:
        value = os.getenv(name)
        if value:
            return value
    return None


def _nextflow_driver_operator(
    *,
    task_id: str,
    task_display_name: str,
    name: str,
    script: str,
    image: str | None,
    env_vars: dict[str, str],
    config_configmap: str,
    params_configmap: str,
) -> KubernetesPodOperator:
    """The pod every Nextflow driver runs in, whichever pipeline it launches.

    Unlike every other operator here a driver runs in the `nextflow` namespace under
    the `nextflow` service account -- that SA carries the Pod Identity for S3 and the
    RBAC to spawn workers -- and it is the only kind that mounts volumes.

    Only the launch script, the pod name and the two ConfigMap *names* differ between
    pipelines. Everything below is topology: which cluster, which filesystem, how long
    to wait. Sharing it is what stops the two drivers drifting apart on the settings
    that are about the deployment rather than about the pipeline.
    """
    workspace = os.getenv("NEXTFLOW_OPERATOR_WORKSPACE_PATH", "/workspace")
    return KubernetesPodOperator(
        task_id=task_id,
        task_display_name=task_display_name,
        name=name,
        namespace=os.getenv("NEXTFLOW_OPERATOR_KUBERNETES_NAMESPACE", "nextflow"),
        service_account_name=os.getenv("NEXTFLOW_OPERATOR_SERVICE_ACCOUNT_NAME", "nextflow"),
        image=image,
        image_pull_policy="IfNotPresent",
        cmds=["/bin/bash", "-c"],
        arguments=[script],
        env_vars={
            "NXF_WORKSPACE": workspace,
            # ANSI progress redraws are unreadable once captured into a task log.
            "NXF_ANSI_LOG": "false",
            "POST_RUN_DRAIN_SECONDS": os.getenv("NEXTFLOW_OPERATOR_DRAIN_SECONDS", "30"),
            # No AWS_* here, unlike the other operators: this pod has Pod Identity,
            # and injecting empty credentials would break the chain.
            #
            # Callers add the per-run values, RUN_TAG among them. nextflow.config
            # reads RUN_TAG via System.getenv too, for `workDir = /workspace/work/
            # ${runTag}` -- so one tag stabilises both the work dir and the launch
            # dir, which is what -resume needs.
            **env_vars,
        },
        volumes=[
            k8s.V1Volume(
                name="workspace",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name=os.getenv("NEXTFLOW_OPERATOR_PVC_NAME", "fsx-nextflow"),
                ),
            ),
            k8s.V1Volume(
                name="nextflow-cfg",
                config_map=k8s.V1ConfigMapVolumeSource(name=config_configmap),
            ),
            k8s.V1Volume(
                name="nextflow-params",
                config_map=k8s.V1ConfigMapVolumeSource(name=params_configmap),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name="workspace", mount_path=workspace),
            k8s.V1VolumeMount(name="nextflow-cfg", mount_path="/etc/nextflow", read_only=True),
            k8s.V1VolumeMount(name="nextflow-params", mount_path="/etc/nextflow-params", read_only=True),
        ],
        # The nodepool is pinned to the FSx AZ; landing elsewhere means a cross-AZ
        # Lustre mount. do-not-disrupt stops Karpenter consolidating the driver away.
        node_selector={"nodepool": os.getenv("NEXTFLOW_OPERATOR_NODEPOOL", "qlin-nextflow")},
        annotations={"karpenter.sh/do-not-disrupt": "true"},
        container_resources=_nextflow_container_resources(),
        get_logs=True,
        # A WGS run takes hours, so the worker slot is released and the triggerer
        # polls instead. Both run as the `airflow` SA, which needs RBAC in the
        # `nextflow` namespace (see apps/nextflow/rbac.yaml in qlin-qa-infra).
        deferrable=True,
        poll_interval=float(os.getenv("NEXTFLOW_OPERATOR_POLL_INTERVAL", "30")),
        # Without this a deferred pod's logs only surface once it finishes, so a
        # multi-hour run looks silent in the UI.
        logging_interval=int(os.getenv("NEXTFLOW_OPERATOR_LOGGING_INTERVAL", "600")),
        # The provider default of 120s is short of a Karpenter cold start plus a
        # multi-GB image pull on a fresh node.
        startup_timeout_seconds=int(os.getenv("NEXTFLOW_OPERATOR_STARTUP_TIMEOUT_SECONDS", "1800")),
        # Backstop: clearing a deferred task does not delete its pod, and a second
        # driver against the same launch dir corrupts the resume cache.
        active_deadline_seconds=int(os.getenv("NEXTFLOW_OPERATOR_ACTIVE_DEADLINE_SECONDS", "86400")),
        # Keep a failed driver for inspection; clean up successful ones.
        on_finish_action="delete_succeeded_pod",
    )


def _nextflow_cleanup_operator(run_tag: str, name: str, image: str | None) -> KubernetesPodOperator:
    """Delete this run's Nextflow scratch after a successful pipeline.

    A WGS run leaves ~200-280 GB in workDir plus the launch dir, and the
    work-cleanup CronJob only sweeps launchdirs older than MAX_AGE_DAYS -- so
    without this, a couple of runs fill the 1.2 TiB filesystem and the failure
    surfaces as ENOSPC on whichever task happens to be writing.

    Only ever wired downstream of a SUCCESSFUL run: the scratch is what `-resume`
    needs, so deleting it after a failure would turn every Airflow retry into a
    full re-run.

    Shared by every Nextflow DAG. It deletes by RUN_TAG on a filesystem all of them
    share, so keeping one copy keeps the `${RUN_TAG:?}` guard in one place too.
    """
    workspace = os.getenv("NEXTFLOW_OPERATOR_WORKSPACE_PATH", "/workspace")
    return KubernetesPodOperator(
        task_id="cleanup_work",
        task_display_name="[K8s] Clean up Nextflow scratch",
        name=name,
        namespace=os.getenv("NEXTFLOW_OPERATOR_KUBERNETES_NAMESPACE", "nextflow"),
        service_account_name=os.getenv("NEXTFLOW_OPERATOR_SERVICE_ACCOUNT_NAME", "nextflow"),
        # Reuse the driver image: it is already cached on these nodes, so this
        # adds no pull.
        image=image,
        image_pull_policy="IfNotPresent",
        cmds=["/bin/bash", "-c"],
        arguments=[_NEXTFLOW_CLEANUP_SCRIPT],
        env_vars={"RUN_TAG": run_tag, "NXF_WORKSPACE": workspace},
        volumes=[
            k8s.V1Volume(
                name="workspace",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name=os.getenv("NEXTFLOW_OPERATOR_PVC_NAME", "fsx-nextflow"),
                ),
            ),
        ],
        volume_mounts=[k8s.V1VolumeMount(name="workspace", mount_path=workspace)],
        node_selector={"nodepool": os.getenv("NEXTFLOW_OPERATOR_NODEPOOL", "qlin-nextflow")},
        container_resources=_container_resources("NEXTFLOW_CLEANUP", cpu="500m", memory="512Mi", memory_limit="1Gi"),
        get_logs=True,
        deferrable=False,
        on_finish_action="delete_succeeded_pod",
    )


class NextflowPostprocessing:
    """Runs the Ferlab Post-processing-Pipeline (VEP / slivar / Exomiser) as a
    Nextflow driver pod, which spawns its own worker pods via Nextflow's k8s executor.
    """

    @staticmethod
    def get_run_postprocessing(input_csv: str, outdir: str, run_tag: str) -> KubernetesPodOperator:
        return _nextflow_driver_operator(
            task_id="run_postprocessing",
            task_display_name="[K8s] Run Nextflow Post-processing",
            name="nextflow-postprocessing-driver",
            script=_NEXTFLOW_DRIVER_SCRIPT,
            image=_nextflow_image("NEXTFLOW_OPERATOR_IMAGE"),
            env_vars={
                "RUN_TAG": run_tag,
                "NXF_INPUT": input_csv,
                "NXF_OUTDIR": outdir,
            },
            config_configmap=os.getenv("NEXTFLOW_OPERATOR_CONFIG_CONFIGMAP", "nextflow-cfg"),
            params_configmap=os.getenv("NEXTFLOW_OPERATOR_PARAMS_CONFIGMAP", "nextflow-params"),
        )

    @staticmethod
    def get_cleanup_work(run_tag: str) -> KubernetesPodOperator:
        return _nextflow_cleanup_operator(
            run_tag,
            name="nextflow-postprocessing-cleanup",
            image=_nextflow_image("NEXTFLOW_OPERATOR_IMAGE"),
        )


class NextflowQualityControl:
    """Runs the Ferlab quality-control-pipeline in its DRAGEN-metrics mode as a
    Nextflow driver pod, which spawns its own worker pods via Nextflow's k8s executor.

    It shares the driver image, the FSx workspace and the node pool with
    `NextflowPostprocessing`, but not its configuration: the post-processing
    `nextflow.config` references `params.save_genotyped` and `params.tools`, and a
    param referenced from a `-c` config but absent from the `-params-file` kills the
    run at config parse. Hence a separate ConfigMap pair.
    """

    @staticmethod
    def get_run_quality_control(
        input_csv: str, outdir: str, dragen_metrics_dir: str, run_tag: str
    ) -> KubernetesPodOperator:
        return _nextflow_driver_operator(
            task_id="run_quality_control",
            task_display_name="[K8s] Run Nextflow Quality Control",
            name="nextflow-quality-control-driver",
            script=_NEXTFLOW_QC_DRIVER_SCRIPT,
            # Both pipelines are baked into one image, so the shared var is the norm.
            # NEXTFLOW_QC_OPERATOR_IMAGE exists to pin QC to an ad-hoc build while a
            # pipeline revision is being tested, without moving post-processing too.
            image=_nextflow_image("NEXTFLOW_QC_OPERATOR_IMAGE", "NEXTFLOW_OPERATOR_IMAGE"),
            env_vars={
                "RUN_TAG": run_tag,
                "NXF_INPUT": input_csv,
                "NXF_OUTDIR": outdir,
                "NXF_DRAGEN_METRICS_DIR": dragen_metrics_dir,
            },
            config_configmap=os.getenv("NEXTFLOW_QC_OPERATOR_CONFIG_CONFIGMAP", "nextflow-qc-cfg"),
            params_configmap=os.getenv("NEXTFLOW_QC_OPERATOR_PARAMS_CONFIGMAP", "nextflow-qc-params"),
        )

    @staticmethod
    def get_cleanup_work(run_tag: str) -> KubernetesPodOperator:
        return _nextflow_cleanup_operator(
            run_tag,
            name="nextflow-quality-control-cleanup",
            image=_nextflow_image("NEXTFLOW_QC_OPERATOR_IMAGE", "NEXTFLOW_OPERATOR_IMAGE"),
        )
