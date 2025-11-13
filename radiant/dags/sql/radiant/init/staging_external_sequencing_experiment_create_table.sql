CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_staging_external_sequencing_experiment }} AS
SELECT
	case_id,
	seq_id,
	task_id,
	analysis_type,
	aliquot,
	patient_id,
	experimental_strategy,
	request_priority,
	MIN(vcf_filepath) AS vcf_filepath,
    MIN(cnv_vcf_filepath) AS cnv_vcf_filepath,
	MIN(exomiser_filepath) AS exomiser_filepath,
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
        c.case_type_code AS analysis_type,
        se.aliquot AS aliquot,
        se.patient_id AS patient_id,
        exp.experimental_strategy_code AS experimental_strategy,
        c.priority_code AS request_priority,
        CASE WHEN d.format_code = 'vcf' AND d.data_type_code='snv' THEN d.url ELSE NULL END AS vcf_filepath,
        CASE WHEN d.format_code = 'vcf' AND d.data_type_code='gcnv' THEN d.url ELSE NULL END AS cnv_vcf_filepath,
        CASE WHEN d.format_code = 'tsv' THEN d.url ELSE NULL END AS exomiser_filepath,
        p.sex_code AS sex,
        IF(p.id = c.proband_id, "proband", f.relationship_to_proband_code) AS family_role,
        IF(p.id = c.proband_id, "affected", f.affected_status_code) AS affected_status,
        se.created_on AS created_at,
        se.updated_on AS updated_at
    FROM
        {{ mapping.clinical_sequencing_experiment }} se
    JOIN {{ mapping.clinical_case }} c ON se.case_id = c.id
    LEFT JOIN {{ mapping.clinical_experiment }} exp ON exp.id = se.experiment_id
    LEFT JOIN {{ mapping.clinical_task_has_sequencing_experiment }} thse ON se.id = thse.sequencing_experiment_id
    LEFT JOIN {{ mapping.clinical_task_has_document }} thd ON thse.task_id = thd.task_id
    LEFT JOIN {{ mapping.clinical_document }} d ON thd.document_id = d.id
    LEFT JOIN {{ mapping.clinical_patient }} p ON se.patient_id = p.id
    LEFT JOIN {{ mapping.clinical_family }} f ON f.family_member_id = p.id
    WHERE (
        (d.format_code = 'vcf' AND d.data_type_code = 'snv')
        OR (d.format_code = 'vcf' AND d.data_type_code = 'gcnv')
        OR (d.format_code = 'tsv' AND d.data_type_code = 'exomiser' AND d.url LIKE '%variants.tsv')
    )
    AND c.status_code in ('in_progress', 'completed')
) se
GROUP BY
    case_id,
    seq_id,
    task_id,
    analysis_type,
    aliquot,
    patient_id,
    experimental_strategy,
    request_priority,
    sex,
    family_role,
    affected_status,
    created_at,
    updated_at