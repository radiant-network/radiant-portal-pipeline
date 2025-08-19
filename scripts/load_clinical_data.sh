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

echo "Loading data..."

copy_cmd() {
  echo "Importing $2..."
  psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -p "$PGPORT" -c "\\copy $1 FROM '$CSV_DIR/$2' DELIMITER ',' CSV HEADER;"
}

for entry in \
  "organization organization.csv" \
  "patient patient.csv" \
  "project project.csv" \
  "case_analysis case_analysis.csv" \
  "cases(id,proband_id,project_id,case_analysis_id,status_code,request_id,performer_lab_id,primary_condition,note,created_on,updated_on) cases.csv" \
  "family_relationship family_relationship.csv" \
  "family family.csv" \
  "observation_coding observation_coding.csv" \
  "sample sample.csv" \
  "experimental_strategy experimental_strategy.csv" \
  "experiment experiment.csv" \
  "sequencing_experiment(id,case_id,patient_id,sample_id,experiment_id,status_code,aliquot,request_id,performer_lab_id,run_name,run_alias,run_date,capture_kit,is_paired_end,read_length,created_on,updated_on) sequencing_experiment.csv" \
  "pipeline pipeline.csv" \
  "task task.csv" \
  "task_has_sequencing_experiment task_has_sequencing_experiments.csv" \
  "document document.csv" \
  "task_has_document task_has_documents.csv"
do
  TABLE=$(echo $entry | awk '{print $1}')
  FILE=$(echo $entry | awk '{print $2}')
  if [ -f "$CSV_DIR/$FILE" ]; then
    copy_cmd "$TABLE" "$FILE"
  else
    echo "Skipping $FILE (not found)..."
  fi
done

echo "✅ All data imported successfully."