#!/bin/sh
set -e

cat /etc/system-release

readonly PLUGINS_DIR="/usr/local/airflow/plugins"

echo "Expanding LD_LIBRARY_PATH..."
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${PLUGINS_DIR}/lib/"
env | grep LD_LIBRARY_PATH
echo "Done expanding LD_LIBRARY_PATH."

echo "Installing Python dependencies..."
pip install ${PLUGINS_DIR}/wheels/*.whl --find-links ${PLUGINS_DIR}/wheels
echo "Done installing Python dependencies."

echo "Configuring Radiant environment..."
export RADIANT_ICEBERG_NAMESPACE="radiant_qa"
env | grep RADIANT_ICEBERG_NAMESPACE
echo "Done configuring Radiant environment."

echo "Exporting PYICEBERG_CATALOG__DEFAULT__TYPE to 'glue'..."
export PYICEBERG_CATALOG__DEFAULT__TYPE=glue
env | grep PYICEBERG_CATALOG__DEFAULT__TYPE
echo "Done setting PYICEBERG_CATALOG__DEFAULT__TYPE."

echo "Startup script done, exiting..."