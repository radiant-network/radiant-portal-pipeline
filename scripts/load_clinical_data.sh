#!/bin/bash

# Usage: ./load_data.sh <PGUSER> <PGHOST> <PGPORT> <PGDATABASE> <CSV_DIR>

set -e  # Exit immediately if a command exits with a non-zero status

PGUSER=$1
PGHOST=$2
PGPORT=$3
PGDATABASE=$4
CSV_DIR=$5

# Prompt for password if not already set in env
if [ -z "$PGPASSWORD" ]; then
  read -s -p "Enter PostgreSQL password: " PGPASSWORD
  echo
fi

export PGPASSWORD

echo "Truncating tables..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -p "$PGPORT" <<SQL
TRUNCATE organization CASCADE;
TRUNCATE patient CASCADE;
TRUNCATE project CASCADE;
TRUNCATE request CASCADE;
TRUNCATE case_analysis CASCADE;
TRUNCATE cases CASCADE;
TRUNCATE family CASCADE;
TRUNCATE observation_coding CASCADE;
TRUNCATE sample CASCADE;
TRUNCATE experiment CASCADE;
TRUNCATE sequencing_experiment CASCADE;
TRUNCATE pipeline CASCADE;
TRUNCATE task CASCADE;
TRUNCATE task_has_sequencing_experiment CASCADE;
TRUNCATE document CASCADE;
TRUNCATE task_has_document CASCADE;
SQL

echo "Loading data..."

copy_cmd() {
  echo "Importing $2..."
  psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -p "$PGPORT" -c "\\copy $1 FROM '$CSV_DIR/$2' DELIMITER ',' CSV HEADER;"
}

copy_cmd organization organization.csv
copy_cmd patient patient.csv
copy_cmd project project.csv
copy_cmd case_analysis case_analysis.csv
copy_cmd "cases(id,proband_id,project_id,case_analysis_id,status_code,request_id,performer_lab_id,primary_condition,note,created_on,updated_on)" cases.csv
copy_cmd family family.csv
copy_cmd observation_coding observation_coding.csv
copy_cmd sample sample.csv
copy_cmd experiment experiment.csv
copy_cmd "sequencing_experiment(id,case_id,patient_id,sample_id,experiment_id,status_code,aliquot,request_id,performer_lab_id,run_name,run_alias,run_date,capture_kit,is_paired_end,read_length,created_on,updated_on)" sequencing_experiment.csv
copy_cmd pipeline pipeline.csv
copy_cmd task task.csv
copy_cmd task_has_sequencing_experiment task_has_sequencing_experiments.csv
copy_cmd document document.csv
copy_cmd task_has_document task_has_documents.csv

echo "✅ All data imported successfully."
