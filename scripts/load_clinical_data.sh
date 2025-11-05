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

  # Extract the first column name from the first line (header)
  local first_header
  first_header=$(head -n 1 "$CSV_DIR/$2" | cut -d',' -f1 | tr -d '[:space:]')

  if [ "$first_header" = "id" ]; then
    echo "⚠️ $CSV_DIR/$2 file contains 'id'"
    return 1
  fi

  psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -p "$PGPORT" -c "\\copy $1 FROM '$CSV_DIR/$2' DELIMITER ',' CSV HEADER;"
}

for entry in \
  "organization(code,name,category_code) organization.csv" \
  "patient(mrn,managing_organization_id,sex_code,date_of_birth) patient.csv" \
  "project(code,name,description) project.csv" \
  "case_analysis(code,name,type_code,panel_id,description) case_analysis.csv" \
  "cases(proband_id,project_id,case_analysis_id,status_code,request_id,performer_lab_id,primary_condition,note,created_on,updated_on) cases.csv" \
  "family_relationship(code,name_en) family_relationship.csv" \
  "family(case_id,family_member_id,relationship_to_proband_code,affected_status_code) family.csv" \
  "obs_categorical(case_id,patient_id,observation_code,coding_system,code_value,onset_code,interpretation_code,note) obs_categorical.csv" \
  "sample(category_code,type_code,parent_sample_id,tissue_site,histology_code,submitter_sample_id) sample.csv" \
  "experimental_strategy(code,name_en) experimental_strategy.csv" \
  "experiment(code,name,experimental_strategy_code,platform_code,description) experiment.csv" \
  "sequencing_experiment(case_id,patient_id,sample_id,experiment_id,status_code,aliquot,request_id,performer_lab_id,run_name,run_alias,run_date,capture_kit,is_paired_end,read_length,created_on,updated_on) sequencing_experiment.csv" \
  "pipeline(description,genome_build) pipeline.csv" \
  "task(type_code,pipeline_id,created_on) task.csv" \
  "task_has_sequencing_experiment(task_id,sequencing_experiment_id) task_has_sequencing_experiments.csv" \
  "document(name,data_category_code,data_type_code,format_code,size,url,hash,created_on) document.csv" \
  "task_has_document(task_id,document_id) task_has_documents.csv"
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