-- HPO terms observed in the probands asked for: 'negative' is a term ruled out, not a feature of the patient.
SELECT DISTINCT CAST(se.seq_id AS BIGINT) AS seq_id, oc.code_value AS hpo_id
FROM ${shared_db}.staging_sequencing_experiment se
JOIN ${clinical_db}.obs_categorical oc ON oc.case_id = se.case_id AND oc.patient_id = se.patient_id
WHERE se.seq_id IN (${cases})
    AND oc.observation_code = 'phenotype'
    AND oc.coding_system = 'HPO'
    AND oc.interpretation_code = 'positive'
