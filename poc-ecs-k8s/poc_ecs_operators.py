import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable, Param
from airflow.providers.amazon.aws.operators import ecs


def parse_list(env_val):
    return [v.strip() for v in env_val.split(",") if v.strip()]


ECS_CLUSTER = Variable.get("AWS_ECS_CLUSTER", default_var=os.environ.get("AWS_ECS_CLUSTER"))
ECS_SUBNETS = parse_list(Variable.get("AWS_ECS_SUBNETS", default_var=os.environ.get("AWS_ECS_SUBNETS", "")))
ECS_SECURITY_GROUPS = parse_list(
    Variable.get("AWS_ECS_SECURITY_GROUPS", default_var=os.environ.get("AWS_ECS_SECURITY_GROUPS", ""))
)

dag_params = {
    "count_to": Param(
        default=2,
        description="A number to count to",
        type="integer",
    )
}


with DAG(
    dag_id="POC-ecs-operators",
    schedule_interval=None,
    catchup=False,
    tags=["radiant", "poc", "manual"],
    dag_display_name="[POC] ECS Operators",
    params=dag_params
) as dag:
    # There's a bug in the 9.2.0 provider that forces to add the container name in the log prefix as well
    run_ecs = ecs.EcsRunTaskOperator(
        task_id="run_ecs_task",
        task_definition="airflow_ecs_operator_task:13",
        cluster=ECS_CLUSTER,
        launch_type="FARGATE",
        awslogs_group="apps-qa/radiant-etl",
        awslogs_region="us-east-1",
        awslogs_stream_prefix="ecs/radiant-operator-qa-etl-container",
        awslogs_fetch_interval=timedelta(seconds=5),
        overrides={
            "containerOverrides": [
                {"name": "radiant-operator-qa-etl-container", "command": ["echo $(python -c \"count_to = {{ params.count_to }}; print(','.join(str(i) for i in range(count_to + 1)))\")"]},
            ]
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ECS_SUBNETS,
                "assignPublicIp": "DISABLED",
                "securityGroups": ECS_SECURITY_GROUPS,
            }
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )
    run_ecs.log.setLevel(logging.DEBUG)
