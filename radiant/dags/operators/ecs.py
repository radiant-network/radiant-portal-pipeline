import os
from datetime import timedelta

from airflow.providers.amazon.aws.operators import ecs

from radiant.dags import ECSEnv


class BaseECSOperator:
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


class ImportGermlineSNVVCF(BaseECSOperator):
    @staticmethod
    def get_create_parquet_files(radiant_namespace: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                pool="import_vcf",
                task_id="create_parquet_files_ecs",
                task_display_name="[ECS] Create Parquet Files",
                overrides={
                    "containerOverrides": [
                        {
                            "name": "radiant-operator-qa-etl-container",
                            "command": [
                                "python /opt/radiant/import_vcf_for_task.py "
                                "--task '{{ params.radiant_task | tojson }}'"
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
            | ImportGermlineSNVVCF._get_ecs_context(
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
            | ImportGermlineSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )


class InitIcebergTables(BaseECSOperator):
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
            | ImportGermlineSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )


class ImportPart(BaseECSOperator):
    @staticmethod
    def get_import_cnv_vcf(radiant_namespace: str, ecs_env: ECSEnv):
        return ecs.EcsRunTaskOperator.partial(
            **dict(
                task_id="import_cnv_vcf_ecs",
                task_display_name="[ECS] Import CNV VCF",
                doc_md="Imports germline CNV VCF files into Iceberg tables. Runs as an ECS task that reads VCF files for each task and writes CNV occurrence data to the Iceberg catalog.",
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
            | ImportGermlineSNVVCF._get_ecs_context(
                ecs_cluster=ecs_env.ECS_CLUSTER,
                ecs_subnets=ecs_env.ECS_SUBNETS,
                ecs_security_groups=ecs_env.ECS_SECURITY_GROUPS,
            )
        )
