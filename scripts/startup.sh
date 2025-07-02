#!/bin/sh
set -e

cat /etc/system-release

readonly PLUGINS_DIR="/usr/local/airflow/plugins"

echo "Installing Python dependencies..."
pip install ${PLUGINS_DIR}/wheels/*.whl --find-links ${PLUGINS_DIR}/wheels
echo "Done installing Python dependencies."

echo "Exporting RADIANT_PYTHON_PATH..."
export RADIANT_PYTHON_PATH="/usr/local/bin/python3"
echo "RADIANT_PYTHON_PATH is now set to: $RADIANT_PYTHON_PATH"

echo "Exporting libraries..."
sudo cp ${PLUGINS_DIR}/lib/libhts.so.1.21 /usr/lib/libhts.so.1.21
sudo chmod +x /usr/lib/libhts.so.1.21
sudo rm -f /usr/lib/libhts.so.3
sudo ln -s /usr/lib/libhts.so.1.21 /usr/lib/libhts.so.3
sudo rm -f /usr/lib/libhts.so
sudo ln -s /usr/lib/libhts.so.1.21 /usr/lib/libhts.so
echo "Done exporting libraries."

echo "Setting up LD_LIBRARY_PATH..."
export LD_LIBRARY_PATH="/usr/lib:$LD_LIBRARY_PATH"
echo "LD_LIBRARY_PATH is now set to: $LD_LIBRARY_PATH"

echo "Configuring Radiant environment..."
export RADIANT_ICEBERG_NAMESPACE="radiant_qa"
env | grep RADIANT_ICEBERG_NAMESPACE
echo "Done configuring Radiant environment."

echo "Exporting PYICEBERG_CATALOG__DEFAULT__TYPE to 'glue'..."
export PYICEBERG_CATALOG__DEFAULT__TYPE=glue
env | grep PYICEBERG_CATALOG__DEFAULT__TYPE
echo "Done setting PYICEBERG_CATALOG__DEFAULT__TYPE."

echo "Startup script done, exiting..."
