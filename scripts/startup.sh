#!/bin/sh
set -e

cat /etc/system-release

readonly PLUGINS_DIR="/usr/local/airflow/plugins"

echo "Expanding LD_LIBRARY_PATH..."
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${PLUGINS_DIR}/lib/"

echo "Installing Python dependencies..."
pip install ${PLUGINS_DIR}/wheels/*.whl --find-links ${PLUGINS_DIR}/wheels