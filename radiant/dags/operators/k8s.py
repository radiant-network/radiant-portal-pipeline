import os

from airflow.decorators import task


class BaseK8SOperator:
    @staticmethod
    def _get_k8s_context(radiant_namespace: str):
        return dict(
            namespace=os.getenv("RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE"),
            image=os.getenv("RADIANT_TASK_OPERATOR_IMAGE"),
            image_pull_policy="IfNotPresent",
            get_logs=True,
            is_delete_operator_pod=True,
            env_vars={
                "AWS_REGION": os.getenv("AWS_REGION"),
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL"),
                "AWS_ALLOW_HTTP": os.getenv("AWS_ALLOW_HTTP"),
                "PYICEBERG_CATALOG__DEFAULT__URI": os.getenv("PYICEBERG_CATALOG__DEFAULT__URI"),
                "PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT": os.getenv("PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT"),
                "PYICEBERG_CATALOG__DEFAULT__TOKEN": os.getenv("PYICEBERG_CATALOG__DEFAULT__TOKEN"),
                "RADIANT_ICEBERG_NAMESPACE": radiant_namespace,
                "PYTHONPATH": os.getenv("RADIANT_TASK_OPERATOR_PYTHONPATH"),
                "LD_LIBRARY_PATH": os.getenv("RADIANT_TASK_OPERATOR_LD_LIBRARY_PATH"),
            },
        )


class ImportGermlineSNVVCF(BaseK8SOperator):
    @staticmethod
    def get_create_parquet_files(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                pool="import_vcf",
                task_id="create_parquet_files_k8s",
                task_display_name="[K8s] Create Parquet Files",
                map_index_template="Task: {{ task.op_kwargs['radiant_task']['task_id'] }}",
                name="import-vcf-for-task",
                do_xcom_push=True,
            )
            | ImportGermlineSNVVCF._get_k8s_context(radiant_namespace)
        )
        def k8s_create_parquet_files(
            radiant_task: dict,
        ):  # `task` is a reserved Airflow keyword, so we use `radiant_task`
            import os

            from radiant.tasks.vcf.snv.germline.process import create_parquet_files

            namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE")
            return create_parquet_files(task=radiant_task, namespace=namespace)

        return k8s_create_parquet_files

    @staticmethod
    def get_commit_partitions(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="commit_partitions_k8s",
                task_display_name="[K8s] Commit Partitions",
                name="commit-partitions",
                do_xcom_push=True,
            )
            | ImportGermlineSNVVCF._get_k8s_context(radiant_namespace),
        )
        def k8s_commit_partitions(table_partitions: dict[str, list[dict]]):
            from radiant.tasks.vcf.snv.germline.process import commit_partitions

            commit_partitions(table_partitions)

        return k8s_commit_partitions


class ImportPart(BaseK8SOperator):
    @staticmethod
    def get_import_cnv_vcf(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="import_cnv_vcf_k8s",
                task_display_name="[K8s] Import CNV VCF",
                name="import-cnv-vcf",
                do_xcom_push=True,
            )
            | ImportPart._get_k8s_context(radiant_namespace)
        )
        def import_cnv_vcf(tasks: list[dict]) -> None:
            import os

            from radiant.tasks.vcf.cnv.germline.process import import_cnv_vcf as _import_cnv_vcf

            namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE")
            _import_cnv_vcf(tasks=tasks, namespace=namespace)

        return import_cnv_vcf


class InitIcebergTables(BaseK8SOperator):
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
    def get_create_germline_variant_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_germline_variant_table_k8s",
                task_display_name="[K8s] Create Germline SNV Variants Table",
                name="create-germline-snv-variants-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_germline_variant_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_germline_variant_table()

        return create_germline_variant_table

    @staticmethod
    def get_create_germline_consequence_table(radiant_namespace: str):
        @task.kubernetes(
            **dict(
                task_id="create_germline_consequence_table_k8s",
                task_display_name="[K8s] Create Germline SNV Consequences Table",
                name="create-germline-snv-consequences-table",
                do_xcom_push=True,
            )
            | InitIcebergTables._get_k8s_context(radiant_namespace)
        )
        def create_germline_consequences_table():
            from radiant.tasks.iceberg import initialization

            initialization.create_germline_consequences_table()

        return create_germline_consequences_table

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
