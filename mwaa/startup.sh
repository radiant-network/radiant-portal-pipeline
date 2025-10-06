#!/bin/sh
set -e

cat /etc/system-release

echo "Setting up IS_AWS..."
export IS_AWS="true"
echo "IS_AWS is now set to: $IS_AWS"

echo "Configuring Radiant environment..."
export RADIANT_ICEBERG_NAMESPACE="radiant_qa"
env | grep RADIANT_ICEBERG_NAMESPACE
echo "Done configuring Radiant environment."

echo "Exporting PYICEBERG_CATALOG__DEFAULT__TYPE to 'glue'..."
export PYICEBERG_CATALOG__DEFAULT__TYPE=glue
env | grep PYICEBERG_CATALOG__DEFAULT__TYPE
echo "Done setting PYICEBERG_CATALOG__DEFAULT__TYPE."

echo "Exporting STARROCKS_BROKER_USE_INSTANCE_PROFILE to 'true'..."
export STARROCKS_BROKER_USE_INSTANCE_PROFILE=true
env | grep STARROCKS_BROKER_USE_INSTANCE_PROFILE
echo "Done setting STARROCKS_BROKER_USE_INSTANCE_PROFILE."

echo "Exporting AWS ECS environment variables..."
export RADIANT_TASK_OPERATOR_TASK_DEFINITION="airflow_ecs_operator_task:13"
export RADIANT_TASK_OPERATOR_LOG_GROUP="apps-qa/radiant-etl"
export RADIANT_TASK_OPERATOR_LOG_REGION="us-east-1"
export RADIANT_TASK_OPERATOR_LOG_PREFIX="ecs/radiant-operator-qa-etl-container"

echo "Startup script done, exiting..."
