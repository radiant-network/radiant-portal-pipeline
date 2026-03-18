INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_somatic_snv_staging_variant_frequency }}
WITH
somatic_sequencings AS (
	SELECT
		case_id,
		part,
		patient_id,
		experimental_strategy,
		MAX(CASE WHEN histology_type = 'tumoral' THEN seq_id END) AS tumor_seq_id,
		MAX(CASE WHEN histology_type = 'normal'  THEN seq_id END) AS normal_seq_id,
		COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN seq_id END) > 0
		AND
		COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN seq_id END) > 0 AS is_tumor_normal
	FROM {{ mapping.starrocks_staging_sequencing_experiment }}
	WHERE analysis_type = 'somatic'
	  AND part=%(part)s
	GROUP BY
		case_id,
		part,
		patient_id,
		experimental_strategy
),
patients_total_count_cohort AS (
    SELECT
		part,
		COUNT(DISTINCT CASE WHEN experimental_strategy = 'wgs' THEN patient_id END) AS cnt_tn_wgs,
		COUNT(DISTINCT CASE WHEN experimental_strategy = 'wxs' THEN patient_id END) AS cnt_tn_wxs
	FROM somatic_sequencings
	WHERE is_tumor_normal = true
	GROUP BY part
),
freqs_tumor AS (
    SELECT
        o.part,
        o.locus_id,
        COUNT(DISTINCT CASE WHEN s.experimental_strategy = 'wgs' THEN s.patient_id END) AS pc_wgs,
        COUNT(DISTINCT CASE WHEN s.experimental_strategy = 'wxs' THEN s.patient_id END) AS pc_wxs
    FROM {{ mapping.starrocks_somatic_snv_occurrence }} o
    JOIN {{ mapping.starrocks_staging_sequencing_experiment }} s ON s.seq_id = o.tumor_seq_id
    WHERE o.part = %(part)s
      AND o.filter = 'PASS'
      AND o.tumor_ad_alt > 2
    GROUP BY o.locus_id, o.part
)
SELECT
    part,
    locus_id,
    pc_wgs                                                          		  AS pc_tn_wgs,
    (SELECT cnt_tn_wgs FROM patients_total_count_cohort)                      AS pn_tn_wgs,
    pc_wgs / NULLIF((SELECT cnt_tn_wgs FROM patients_total_count_cohort), 0)  AS pf_tn_wgs,
    pc_wxs                                                          	      AS pc_tn_wxs,
    (SELECT cnt_tn_wxs FROM patients_total_count_cohort)                      AS pn_tn_wxs,
    pc_wxs / NULLIF((SELECT cnt_tn_wxs FROM patients_total_count_cohort), 0)  AS pf_tn_wxs,
    0 AS pc_to_wgs,
    0 AS pn_to_wgs,
	0 AS pf_to_wgs,
	0 AS pc_to_wxs,
	0 AS pn_to_wxs,
	0 AS pf_to_wxs
FROM freqs_tumor