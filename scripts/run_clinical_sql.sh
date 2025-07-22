#!/bin/bash

# Usage: ./load_data.sh <PGUSER> <PGHOST> <PGPORT> <PGDATABASE> <CSV_DIR>

set -e  # Exit immediately if a command exits with a non-zero status

PGUSER=$1
PGHOST=$2
PGPORT=$3
PGDATABASE=$4

# Prompt for password if not already set in env
if [ -z "$PGPASSWORD" ]; then
  read -s -p "Enter PostgreSQL password: " PGPASSWORD
  echo
fi

export PGPASSWORD

echo "Running SQL..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -p "$PGPORT" <<SQL
-- Placeholder for SQL commands
SQL

echo "✅ All SQL ran successfully."


