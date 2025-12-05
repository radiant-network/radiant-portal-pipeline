CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_staging_external_sequencing_experiment }} AS
with sequencing_context AS (
	SELECT
    se.id,
    se.aliquot,
    se.sample_id,
    se.experimental_strategy_code,
    se.created_on,
    se.updated_on AS updated_on,
    ANY_VALUE(c.case_type_code) as analysis_type,
    CASE
        MIN(
                CASE priority_code
                    WHEN 'stat' THEN 1
                    WHEN 'asap' THEN 2
                    WHEN 'urgent' THEN 3
                    ELSE 4 -- routine
                END
            )
            WHEN 1 THEN 'stat'
            WHEN 2 THEN 'asap'
            WHEN 3 THEN 'urgent'
	        ELSE 'routine'
        END AS priority_code
	FROM  {{ mapping.clinical_sequencing_experiment }} se
	LEFT JOIN {{ mapping.clinical_case_has_sequencing_experiment }} chse ON chse.sequencing_experiment_id = se.id
	JOIN {{ mapping.clinical_case }} c ON chse.case_id = c.id
	WHERE c.status_code in ('in_progress', 'completed')
	GROUP BY
		se.id,
	    se.aliquot,
	    se.sample_id,
	    se.experimental_strategy_code,
	    se.created_on,
	    se.updated_on
),
sequencing_experiments AS (
	SELECT
	    t.id AS task_id,
	    t.task_type_code AS task_type,
	    se.id AS seq_id,
	    se.sample_id,
	    se.aliquot,
	    se.analysis_type,
	    se.experimental_strategy_code AS experimental_strategy,
        COALESCE(se.priority_code, 'routine') AS request_priority,
	    ANY_VALUE(CASE WHEN d.format_code = 'vcf' AND d.data_type_code='snv' THEN d.url ELSE NULL END) AS vcf_filepath,
	    ANY_VALUE(CASE WHEN d.format_code = 'vcf' AND d.data_type_code='gcnv' THEN d.url ELSE NULL END) AS cnv_vcf_filepath,
	    ANY_VALUE(CASE WHEN d.format_code = 'tsv' THEN d.url ELSE NULL END) AS exomiser_filepath,
        se.created_on AS created_at,
        IF(tctx.case_id IS NOT NULL, c.updated_on, se.updated_on) AS updated_at
	FROM
	    sequencing_context se
	LEFT JOIN {{ mapping.clinical_task_context }} tctx ON tctx.sequencing_experiment_id = se.id
	LEFT JOIN {{ mapping.clinical_case }} c ON tctx.case_id = c.id
	LEFT JOIN {{ mapping.clinical_task }} t ON t.id = tctx.task_id
	LEFT JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = t.id AND thd.type = 'output'
	LEFT JOIN {{ mapping.clinical_document }} d ON d.id = thd.document_id
	WHERE (
		(d.format_code = 'vcf' AND t.task_type_code = 'radiant_germline_annotation')
		OR (d.format_code = 'vcf' AND t.task_type_code = 'alignment_germline_variant_calling')
		OR (d.url LIKE '%variants.tsv' AND t.task_type_code = 'exomiser')
	)
	GROUP BY
	    t.id,
	    t.task_type_code,
		se.id,
		se.sample_id,
		se.aliquot,
		se.analysis_type,
		se.experimental_strategy_code,
		se.priority_code,
		se.created_on,
		updated_at
)
SELECT
	s.seq_id,
	s.task_id,
	s.task_type,
	s.analysis_type,
	s.aliquot,
	p.id as patient_id,
	s.experimental_strategy,
	s.request_priority,
	s.vcf_filepath,
    s.cnv_vcf_filepath,
	s.exomiser_filepath,
	p.sex_code AS sex,
	COALESCE(f.relationship_to_proband_code, "proband") AS family_role,
    COALESCE(f.affected_status_code, "affected") AS affected_status,
	s.created_at,
	s.updated_at
FROM sequencing_experiments s
LEFT JOIN {{ mapping.clinical_sample }} sa ON sa.id = s.sample_id
LEFT JOIN {{ mapping.clinical_patient }} p ON p.id = sa.patient_id
LEFT JOIN {{ mapping.clinical_family }} f ON f.family_member_id = p.id
