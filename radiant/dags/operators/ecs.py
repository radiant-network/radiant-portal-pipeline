import os
from datetime import timedelta

from airflow.providers.amazon.aws.operators import ecs

from radiant.dags import ECSEnv

# Propagated to the SNV extraction tasks so a memory-constrained environment can shrink the
# TableAccumulator flush threshold, which is what sets an extraction task's memory floor. An
# empty value means "use the default" (see `resolve_parquet_file_size_mb`); the literal default
# is deliberately not repeated here, since importing it would pull pyarrow/pyiceberg into the
# Airflow interpreter, which does not have them.
_PARQUET_FILE_SIZE_ENV = {
    "name": "RADIANT_PARQUET_FILE_SIZE_MB",
    "value": os.getenv("RADIANT_PARQUET_FILE_SIZE_MB", ""),
}


class RadiantTaskECSOperator:
    @staticmethod
    def _get_ecs_context(ecs_cluster: str, ecs_subnets: list[str], ecs_security_groups: list[str]):
        return dict(
            cluster=ecs_cluster,
            launch_type="FARGATE",
            task_definition=os.getenv("RADIANT_TASK_OPERATOR_TASK_DEFINITION"),
            awslogs_group=os.getenv("RADIANT_TASK_OPERATOR_LOG_GROUP"),
            awslogs_region=os.getenv("RADIANT_TASK_OPERATOR_LOG_REGION"),
            # There's a bug in the 9.2.0 provider that forces to add the container name as well
            awslogs_stream_prefix=os.getenv("RADIANT_TASK_OPERATOR_LOG_PREFIX"),
            awslogs_fetch_interval=timedelta(seconds=5),
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": ecs_subnets,
                    "assignPublicIp": "DISABLED",
                    "securityGroups": ecs_security_groups,
                }
            },
            aws_conn_id="aws_default",
        )


class ImportSNVVCF(RadiantTaskECSOperator):
    @staticmethod
    def get_create_germline_parquet_files(radiant_namespace: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                pool="import_vcf",
                task_id="create_germline_parquet_files_ecs",
                task_display_name="[ECS] Create Germline Parquet Files",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": [
                                "python /opt/radiant/import_germline_snv_vcf_for_task.py "
                                "--task '{{ params.radiant_task | tojson }}'"
                            ],
                            "environment": [
                                {"name": "PYTHONPATH", "value": "/opt/radiant"},
                                {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                                {"name": "RADIANT_ICEBERG_NAMESPACE", "value": radiant_namespace},
                                {"name": "PYICEBERG_CATALOG__DEFAULT__TYPE", "value": "glue"},
                                {"name": "STARROCKS_BROKER_USE_INSTANCE_PROFILE", "value": "true"},
                                _PARQUET_FILE_SIZE_ENV,
                            ],
                        }
                    ]
                },
            )
            | ImportSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )

    @staticmethod
    def get_create_somatic_parquet_files(radiant_namespace: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                pool="import_vcf",
                task_id="create_somatic_parquet_files_ecs",
                task_display_name="[ECS] Create Somatic Parquet Files",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": [
                                "python /opt/radiant/import_somatic_snv_vcf_for_task.py "
                                "--task '{{ params.radiant_task | tojson }}'"
                            ],
                            "environment": [
                                {"name": "PYTHONPATH", "value": "/opt/radiant"},
                                {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                                {"name": "RADIANT_ICEBERG_NAMESPACE", "value": radiant_namespace},
                                {"name": "PYICEBERG_CATALOG__DEFAULT__TYPE", "value": "glue"},
                                {"name": "STARROCKS_BROKER_USE_INSTANCE_PROFILE", "value": "true"},
                                _PARQUET_FILE_SIZE_ENV,
                            ],
                        }
                    ]
                },
            )
            | ImportSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )

    @staticmethod
    def get_commit_partitions(radiant_namespace: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                task_id="ecs_commit_partitions",
                task_display_name="[ECS] Commit Partitions",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": [
                                "python "
                                "/opt/radiant/commit_partitions.py --table_partitions '{{ params.table_partitions }}'"
                            ],
                            "environment": [
                                {"name": "PYTHONPATH", "value": "/opt/radiant"},
                                {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                                {"name": "RADIANT_ICEBERG_NAMESPACE", "value": radiant_namespace},
                                {"name": "PYICEBERG_CATALOG__DEFAULT__TYPE", "value": "glue"},
                                {"name": "STARROCKS_BROKER_USE_INSTANCE_PROFILE", "value": "true"},
                            ],
                        }
                    ]
                },
            )
            | ImportSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )


class InitIcebergTables(RadiantTaskECSOperator):
    @staticmethod
    def get_init_iceberg(radiant_namespace: str, table_name: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator(
            **dict(
                task_id=f"init_iceberg_{table_name}",
                task_display_name=f"[ECS] Init Iceberg Database: {table_name}",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": [f"python /opt/radiant/init_iceberg_table.py --table_name '{table_name}'"],
                            "environment": [
                                {"name": "PYTHONPATH", "value": "/opt/radiant"},
                                {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                                {"name": "RADIANT_ICEBERG_NAMESPACE", "value": radiant_namespace},
                                {"name": "PYICEBERG_CATALOG__DEFAULT__TYPE", "value": "glue"},
                            ],
                        }
                    ]
                },
            )
            | ImportSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )


class ImportPart(RadiantTaskECSOperator):
    @staticmethod
    def get_import_cnv_vcf(radiant_namespace: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                task_id="import_cnv_vcf_ecs",
                task_display_name="[ECS] Import CNV VCF",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": ["python /opt/radiant/import_cnv_vcf.py --tasks '{{ params.stored_tasks }}'"],
                            "environment": [
                                {"name": "PYTHONPATH", "value": "/opt/radiant"},
                                {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                                {"name": "RADIANT_ICEBERG_NAMESPACE", "value": radiant_namespace},
                                {"name": "PYICEBERG_CATALOG__DEFAULT__TYPE", "value": "glue"},
                            ],
                        }
                    ]
                },
            )
            | ImportSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )

    @staticmethod
    def get_import_somatic_cnv_vcf(radiant_namespace: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                task_id="import_somatic_cnv_vcf_ecs",
                task_display_name="[ECS] Import Somatic CNV VCF",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": [
                                "python /opt/radiant/import_somatic_cnv_vcf.py --tasks '{{ params.stored_tasks }}'"
                            ],
                            "environment": [
                                {"name": "PYTHONPATH", "value": "/opt/radiant"},
                                {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                                {"name": "RADIANT_ICEBERG_NAMESPACE", "value": radiant_namespace},
                                {"name": "PYICEBERG_CATALOG__DEFAULT__TYPE", "value": "glue"},
                            ],
                        }
                    ]
                },
            )
            | ImportSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )

    @staticmethod
    def get_cleanup(ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                task_id="cleanup_tasks_files",
                task_display_name="[ECS] Cleanup tasks files",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": ["python /opt/radiant/cleanup.py --path '{{ params.stored_tasks }}'"],
                            "environment": [
                                {"name": "PYTHONPATH", "value": "/opt/radiant"},
                                {"name": "LD_LIBRARY_PATH", "value": "/usr/local/lib:$LD_LIBRARY_PATH"},
                            ],
                        }
                    ]
                },
            )
            | ImportSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )


class CheckDataIntegrity:
    """Runs dbt data-quality checks. Uses its own ECS task definition, as we use
    a Docker image specific to dbt instead of the standard radiant-task image."""

    @staticmethod
    def get_run_dbt(run_results_s3_uri: str, junit_s3_uri: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator(
            task_id="run_dbt",
            task_display_name="[ECS] Run dbt data tests",
            cluster=ecs_env.ECS_CLUSTER,
            launch_type="FARGATE",
            task_definition=os.getenv("RADIANT_DBT_TASK_DEFINITION"),
            awslogs_group=os.getenv("RADIANT_DBT_LOG_GROUP"),
            awslogs_region=os.getenv("RADIANT_DBT_LOG_REGION"),
            awslogs_stream_prefix=os.getenv("RADIANT_DBT_LOG_PREFIX"),
            awslogs_fetch_interval=timedelta(seconds=5),
            overrides={
                "containerOverrides": [
                    {
                        "name": "radiant-dbt-container",
                        "environment": [
                            {"name": "RUN_RESULTS_S3_URI", "value": run_results_s3_uri},
                            {"name": "JUNIT_S3_URI", "value": junit_s3_uri},
                        ],
                    }
                ]
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": ecs_env.ECS_SUBNETS,
                    "assignPublicIp": "DISABLED",
                    "securityGroups": ecs_env.ECS_SECURITY_GROUPS,
                }
            },
            aws_conn_id="aws_default",
        )
