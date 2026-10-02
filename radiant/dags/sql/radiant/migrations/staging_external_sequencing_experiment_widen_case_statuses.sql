-- Recreate `staging_external_sequencing_experiment` so it reads every case status except the ignored ones.
DROP VIEW IF EXISTS staging_external_sequencing_experiment;
