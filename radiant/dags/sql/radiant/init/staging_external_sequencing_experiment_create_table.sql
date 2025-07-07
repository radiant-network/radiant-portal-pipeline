CREATE VIEW IF NOT EXISTS {{ params.starrocks_staging_external_sequencing_experiment }} AS
SELECT
	case_id,
	seq_id,
	task_id,
	analysis_type,
	aliquot,
	patient_id,
	experimental_strategy,
	request_id,
	request_priority,
	MIN(vcf_filepath) AS vcf_filepath,
	array_filter(ARRAY_AGG(exomiser_filepath), x -> x IS NOT NULL) AS exomiser_filepaths,
	sex,
	family_role,
	affected_status,
	created_at,
	updated_at
FROM (
    SELECT
        se.case_id AS case_id,
        se.id AS seq_id,
        thse.task_id AS task_id,
        ca.type_code AS analysis_type,
        se.aliquot AS aliquot,
        se.patient_id AS patient_id,
        exp.experimental_strategy_code AS experimental_strategy,
        se.request_id AS request_id,
        r.priority_code AS request_priority,
        CASE WHEN d.format_code = 'vcf' THEN d.url ELSE NULL END AS vcf_filepath,
	    CASE WHEN d.format_code = 'tsv' THEN d.url ELSE NULL END AS exomiser_filepath,
        p.sex_code AS sex,
        IF(p.id = c.proband_id, "proband", f.relationship_to_proband_code) AS family_role,
        IF(p.id = c.proband_id, "affected", f.affected_status_code) AS affected_status,
        se.created_on AS created_at,
        se.updated_on AS updated_at
    FROM
        {{ params.clinical_sequencing_experiment }} se
    JOIN {{ params.clinical_case }} c ON se.case_id = c.id
    LEFT JOIN {{ params.clinical_experiment }} exp ON exp.id = se.experiment_id
    LEFT JOIN {{ params.clinical_case_analysis }} ca ON ca.id = c.case_analysis_id
    LEFT JOIN {{ params.clinical_task_has_sequencing_experiment }} thse ON se.id = thse.sequencing_experiment_id
    LEFT JOIN {{ params.clinical_task_has_document }} thd ON thse.task_id = thd.task_id
    LEFT JOIN {{ params.clinical_document }} d ON thd.document_id = d.id
    LEFT JOIN {{ params.clinical_patient }} p ON se.patient_id = p.id
    LEFT JOIN {{ params.clinical_family }} f ON f.family_member_id = p.id
    LEFT JOIN {{ params.clinical_request }} r ON se.request_id = r.id
    WHERE (d.format_code = 'vcf' AND d.data_type_code = 'snv')
       OR (d.format_code = 'tsv' AND d.data_type_code = 'exomiser')
      AND c.status_code in ('active', 'completed')
) se
GROUP BY
    case_id,
    seq_id,
    task_id,
    analysis_type,
    aliquot,
    patient_id,
    experimental_strategy,
    request_id,
    request_priority,
    sex,
    family_role,
    affected_status,
    created_at,
    updated_at