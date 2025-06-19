#!/bin/sh
set -e

readonly PLUGINS_DIR="/usr/local/airflow/plugins"
if [[ "${MWAA_AIRFLOW_COMPONENT}" == "worker" ]]
then
     export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${PLUGINS_DIR}/lib/"
     echo "Installing Python dependencies..."
     unzip -o ${PLUGINS_DIR}/plugins.zip -d ${PLUGINS_DIR}
     pip install ${PLUGINS_DIR}/wheels/*.whl --find-links ${PLUGINS_DIR}/wheels
fi
