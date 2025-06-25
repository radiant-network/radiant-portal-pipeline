#!/bin/sh
set -e

readonly PLUGINS_DIR="/usr/local/airflow/plugins"
if [[ "${MWAA_AIRFLOW_COMPONENT}" == "worker" ]]
then
    export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${PLUGINS_DIR}/lib/"
    echo "Installing Python dependencies..."
    unzip -o ${PLUGINS_DIR}/plugins.zip -d ${PLUGINS_DIR}
    pip install ${PLUGINS_DIR}/wheels/*.whl --find-links ${PLUGINS_DIR}/wheels
else
    # We need to install this manually for components that are not workers.
    # Workers will get this from the wheels directory to speed up the installation.
    pip install --upgrade apache-airflow-providers-common-sql==1.21.0
fi