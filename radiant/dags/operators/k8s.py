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
    def get_run_dbt(run_results_s3_uri: str, junit_s3_uri: str) -> KubernetesPodOperator:
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
