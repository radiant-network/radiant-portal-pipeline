import os
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators import ecs

# Helper to split and clean list values
def parse_list(env_val):
    return [v.strip() for v in env_val.split(",") if v.strip()]

# Prefer Airflow Variables, fallback to Environment variables
ECS_CLUSTER = Variable.get("AWS_ECS_CLUSTER", default_var=os.environ.get("AWS_ECS_CLUSTER"))
ECS_SUBNETS = parse_list(Variable.get("AWS_ECS_SUBNETS", default_var=os.environ.get("AWS_ECS_SUBNETS", "")))
ECS_SECURITY_GROUPS = parse_list(Variable.get("AWS_ECS_SECURITY_GROUPS", default_var=os.environ.get("AWS_ECS_SECURITY_GROUPS", "")))


with DAG(
    dag_id="POC-ecs-operators",
    schedule_interval=None,
    catchup=False,
    tags=["radiant", "poc", "manual"],
    dag_display_name="[POC] ECS Operators",
) as dag:
    run_ecs = ecs.EcsRunTaskOperator(
        task_id="run_ecs_task",
        task_definition="airflow_ecs_operator_task:6",
        cluster=ECS_CLUSTER,
        launch_type="FARGATE",
        awslogs_group="apps-qa/radiant-etl-etl",
        awslogs_region="us-east-1",
        awslogs_stream_prefix="ecs/radiant-etl-operator-qa-container",
        overrides={
            "containerOverrides": [
                {
                    "name": "radiant-etl-operator-qa-container",
                    "command": [
                        "python", "-m", "pip", "freeze"
                    ]
                }
            ]
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ECS_SUBNETS,
                "assignPublicIp": "DISABLED",
                "securityGroups": ECS_SECURITY_GROUPS
            }
        },
        aws_conn_id="aws_default",
        region_name="us-east-1"
    )
