CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_staging_sequencing_experiment_delta }} AS
WITH sequencing_delta AS (
    SELECT
        sse.*
    FROM {{ mapping.starrocks_staging_external_sequencing_experiment }} sse
    LEFT ANTI JOIN {{ mapping.starrocks_staging_sequencing_experiment }} existing
    ON
        sse.case_id = existing.case_id AND
        sse.seq_id = existing.seq_id AND
        sse.task_id = existing.task_id AND
        sse.updated_at <= existing.updated_at
),
patient_part AS (
	SELECT
		MAX(se.part) as part,
		sd.patient_id,
		sd.experimental_strategy
	FROM sequencing_delta sd
	LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
	ON  se.patient_id = sd.patient_id
	AND se.experimental_strategy = sd.experimental_strategy
	GROUP BY
		sd.patient_id,
		sd.experimental_strategy
),
enriched_patient AS (
	SELECT
		sd.*,
		pp.part as patient_part
	FROM sequencing_delta sd
	LEFT JOIN patient_part pp
	ON  pp.patient_id = sd.patient_id
	AND pp.experimental_strategy = sd.experimental_strategy
),
case_part AS (
	SELECT
		MAX(se.part) as part,
		sd.case_id,
		sd.experimental_strategy
	FROM sequencing_delta sd
	LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
	ON  se.case_id = sd.case_id
	AND se.experimental_strategy = sd.experimental_strategy
	GROUP BY
		sd.case_id,
		sd.experimental_strategy
),
enriched_case AS (
	SELECT
		ep.*,
		cp.part as case_part
	FROM enriched_patient ep
	LEFT JOIN case_part cp
	ON  cp.case_id = ep.case_id
	AND cp.experimental_strategy = ep.experimental_strategy
),
max_part AS (
	SELECT
		MAX(se.part) as part,
		sd.experimental_strategy
	FROM sequencing_delta sd
	LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
	ON se.experimental_strategy = sd.experimental_strategy
	GROUP BY
		sd.experimental_strategy
),
enriched_max_part AS (
	SELECT
		ec.*,
		mp.part as max_part
	FROM enriched_case ec
	LEFT JOIN max_part mp
	ON mp.experimental_strategy = ec.experimental_strategy
),
final_data AS (
    SELECT
        emp.*,
        se.max_count as max_count
    FROM enriched_max_part emp
    LEFT JOIN (
    	SELECT
			part,
			experimental_strategy,
			COUNT(1) as max_count
		FROM {{ mapping.starrocks_staging_sequencing_experiment }}
		GROUP BY part, experimental_strategy
    ) se
    ON se.part = emp.max_part
)
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
    vcf_filepath,
    cnv_vcf_filepath,
    exomiser_filepath,
    sex,
    family_role,
    affected_status,
    created_at,
    updated_at,
    patient_part,
    case_part,
    max_part,
    max_count
FROM final_data
