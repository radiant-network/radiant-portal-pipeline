CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_staging_external_sequencing_experiment }} AS
with sequencing_experiments AS (
	SELECT
	    c.id AS case_id,
	    c.proband_id,
	    se.id AS seq_id,
	    se.sample_id,
	    se.aliquot,
	    c.case_type_code AS analysis_type,
	    se.experimental_strategy_code AS experimental_strategy,
        c.priority_code AS request_priority,
	    MAX(CASE WHEN t.task_type_code = 'radiant_germline_annotation' THEN t.id ELSE NULL END) AS task_id,
	    ANY_VALUE(CASE WHEN d.format_code = 'vcf' AND d.data_type_code='snv' THEN d.url ELSE NULL END) AS vcf_filepath,
	    ANY_VALUE(CASE WHEN d.format_code = 'vcf' AND d.data_type_code='gcnv' THEN d.url ELSE NULL END) AS cnv_vcf_filepath,
	    ANY_VALUE(CASE WHEN d.format_code = 'tsv' THEN d.url ELSE NULL END) AS exomiser_filepath,
        se.created_on AS created_at,
        se.updated_on AS updated_at
	FROM
	    {{ mapping.clinical_sequencing_experiment }} se
	LEFT JOIN {{ mapping.clinical_task_context }} tctx ON tctx.sequencing_experiment_id = se.id
	JOIN {{ mapping.clinical_case }} c ON tctx.case_id = c.id
	LEFT JOIN {{ mapping.clinical_task }} t ON t.id = tctx.task_id
	LEFT JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = t.id AND thd.type = 'output'
	LEFT JOIN {{ mapping.clinical_document }} d ON d.id = thd.document_id
	WHERE (
		(d.format_code = 'vcf' AND t.task_type_code = 'radiant_germline_annotation')
		OR (d.format_code = 'vcf' AND t.task_type_code = 'alignment_germline_variant_calling')
		OR (d.url LIKE '%variants.tsv' AND t.task_type_code = 'exomiser')
	)
	AND c.status_code in ('in_progress', 'completed')
	GROUP BY
		c.id,
		c.proband_id,
		se.id,
		se.sample_id,
		se.aliquot,
		c.case_type_code,
		se.experimental_strategy_code,
		c.priority_code,
		se.created_on,
		se.updated_on
)
SELECT
	s.case_id,
	s.seq_id,
	s.task_id,
	s.analysis_type,
	s.aliquot,
	p.id as patient_id,
	s.experimental_strategy,
	s.request_priority,
	s.vcf_filepath,
    s.cnv_vcf_filepath,
	s.exomiser_filepath,
	p.sex_code AS sex,
	IF(p.id = s.proband_id, "proband", f.relationship_to_proband_code) AS family_role,
    IF(p.id = s.proband_id, "affected", f.affected_status_code) AS affected_status,
	s.created_at,
	s.updated_at
FROM sequencing_experiments s
LEFT JOIN {{ mapping.clinical_sample }} sa ON sa.id = s.sample_id
LEFT JOIN {{ mapping.clinical_patient }} p ON p.id = sa.patient_id
LEFT JOIN {{ mapping.clinical_family }} f ON f.family_member_id = p.id
