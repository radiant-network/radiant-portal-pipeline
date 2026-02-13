CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_staging_sequencing_experiment_delta }} AS
WITH sequencing_delta AS (
    SELECT
        sse.*
    FROM {{ mapping.starrocks_staging_external_sequencing_experiment }} sse
    LEFT ANTI JOIN {{ mapping.starrocks_staging_sequencing_experiment }} existing
    ON
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
seq_part AS (
	SELECT
		MAX(se.part) as part,
		sd.seq_id,
		sd.experimental_strategy
	FROM sequencing_delta sd
	LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
	ON  se.seq_id = sd.seq_id
	AND se.experimental_strategy = sd.experimental_strategy
	GROUP BY
		sd.seq_id,
		sd.experimental_strategy
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
family_part AS (
	SELECT
		MAX(se.part) as part,
		sd.family_id,
		sd.experimental_strategy
	FROM sequencing_delta sd
	LEFT JOIN {{ mapping.starrocks_staging_sequencing_experiment }} se
	ON  se.family_id = sd.family_id
	AND se.experimental_strategy = sd.experimental_strategy
	GROUP BY
		sd.family_id,
		sd.experimental_strategy
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
enriched_data AS (
	SELECT
		sd.*,
		pp.part as patient_part,
        sp.part as seq_part,
        cp.part as case_part,
        fp.part as family_part,
        mp.part as max_part
	FROM sequencing_delta sd
	LEFT JOIN patient_part pp
	    ON  pp.patient_id = sd.patient_id
	    AND pp.experimental_strategy = sd.experimental_strategy
    LEFT JOIN seq_part sp
        ON  sp.seq_id = sd.seq_id
        AND sp.experimental_strategy = sd.experimental_strategy
    LEFT JOIN case_part cp
        ON  cp.case_id = sd.case_id
        AND cp.experimental_strategy = sd.experimental_strategy
    LEFT JOIN family_part fp
        ON  fp.family_id = sd.family_id
        AND fp.experimental_strategy = sd.experimental_strategy
	LEFT JOIN max_part mp
	    ON mp.experimental_strategy = sd.experimental_strategy
),
final_data AS (
    SELECT
        ed.*,
        se.max_count as max_count
    FROM enriched_data ed
    LEFT JOIN (
    	SELECT
			part,
			experimental_strategy,
			COUNT(1) as max_count
		FROM {{ mapping.starrocks_staging_sequencing_experiment }}
		GROUP BY part, experimental_strategy
    ) se
    ON se.part = ed.max_part
)
SELECT
    case_id,
    seq_id,
    task_id,
    task_type,
    analysis_type,
    aliquot,
    patient_id,
    experimental_strategy,
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
    seq_part,
    case_part,
    family_part,
    max_part,
    max_count
FROM final_data
