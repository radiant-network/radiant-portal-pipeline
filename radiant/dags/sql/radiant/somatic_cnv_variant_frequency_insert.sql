-- RAD-120: in-cohort frequency of each distinct somatic CNV interval (`cnv_id`) of the tenant. Same matching rule
-- and same full recompute as germline_cnv_variant_frequency_insert.sql, which carries the comments on the binning.
-- Only the tumor-only cohort (`_to_`): every somatic CNV comes from a tumor-only task today. The `_tn_` columns are
-- added when tumor-normal CNVs are ingested; until then a tumor-normal task belongs to no cohort.
INSERT OVERWRITE {{ mapping.starrocks_somatic_cnv_variant_frequency }}
WITH
-- Tumor-only verdict at task grain, as in somatic_snv_staging_variant_freq_insert.sql.
somatic_task_kinds AS (
    SELECT task_id,
           COUNT(DISTINCT CASE WHEN histology_type = 'tumoral' THEN aliquot END) AS n_tumoral,
           COUNT(DISTINCT CASE WHEN histology_type = 'normal'  THEN aliquot END) AS n_normal
    FROM {{ mapping.starrocks_staging_sequencing_experiment }}
    WHERE analysis_type = 'somatic'
      AND tenant_code = %(tenant_code)s
    GROUP BY task_id
),
somatic_tasks AS (
    SELECT DISTINCT s.task_id,
           s.seq_id,
           s.patient_id,
           s.experimental_strategy,
           k.n_tumoral = 1 AND k.n_normal = 0 AS is_tumor_only
    FROM {{ mapping.starrocks_staging_sequencing_experiment }} s
    JOIN somatic_task_kinds k ON k.task_id = s.task_id
    WHERE s.analysis_type = 'somatic'
      AND s.tenant_code = %(tenant_code)s
),
-- Every occurrence of the tenant, flagged instead of filtered (RAD-57).
occurrences AS (
    SELECT o.cnv_id,
           o.filter = 'PASS' AS qualifies,
           t.patient_id,
           t.experimental_strategy
    FROM {{ mapping.starrocks_somatic_cnv_occurrence }} o
    JOIN somatic_tasks t ON t.task_id = o.task_id AND t.seq_id = o.seq_id
    WHERE t.is_tumor_only
),
intervals AS (
    SELECT cnv_id,
           ANY_VALUE(chromosome) AS chromosome,
           ANY_VALUE(`start`)    AS `start`,
           ANY_VALUE(`end`)      AS `end`,
           CASE WHEN ANY_VALUE(`type`) IN ('GAIN', 'GAINLOH') THEN 'GAIN' ELSE ANY_VALUE(`type`) END AS match_class
    FROM {{ mapping.starrocks_somatic_cnv_occurrence }}
    GROUP BY cnv_id
),
binned AS (
    SELECT cnv_id, chromosome, `start`, `end`, match_class,
           FLOOR(LOG2(`end` - `start`))                              AS len_bucket,
           FLOOR(`start` / POW(2, FLOOR(LOG2(`end` - `start`))))    AS pos_bin
    FROM intervals
    WHERE `end` > `start`
),
probe AS (
    SELECT d.cnv_id, d.chromosome, d.match_class, d.`start`, d.`end`,
           d.len_bucket + b.db                                     AS t_len_bucket,
           FLOOR(d.`start` / POW(2, d.len_bucket + b.db)) + p.dp   AS t_pos_bin
    FROM binned d
    CROSS JOIN (SELECT -1 AS db UNION ALL SELECT 0 UNION ALL SELECT 1) b
    CROSS JOIN (SELECT -1 AS dp UNION ALL SELECT 0 UNION ALL SELECT 1) p
),
-- 0.8 = reciprocal overlap threshold, lossless with the +-1 bins only for a threshold >= 0.75.
matches AS (
    SELECT p.cnv_id AS query_id, t.cnv_id AS match_id
    FROM probe p
    JOIN binned t
      ON  t.chromosome  = p.chromosome
      AND t.match_class = p.match_class
      AND t.len_bucket  = p.t_len_bucket
      AND t.pos_bin     = p.t_pos_bin
    WHERE LEAST(p.`end`, t.`end`) - GREATEST(p.`start`, t.`start`) >= 0.8 * (p.`end` - p.`start`)
      AND LEAST(p.`end`, t.`end`) - GREATEST(p.`start`, t.`start`) >= 0.8 * (t.`end` - t.`start`)
),
carriers AS (
    SELECT DISTINCT cnv_id, patient_id, experimental_strategy
    FROM occurrences
    WHERE qualifies
),
counts AS (
    SELECT m.query_id AS cnv_id,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wgs' THEN c.patient_id END) AS pc_to_wgs,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wxs' THEN c.patient_id END) AS pc_to_wxs
    FROM matches m
    JOIN carriers c ON c.cnv_id = m.match_id
    GROUP BY m.query_id
),
-- Patients with at least one tumor-only CNV occurrence, whatever its filter.
denominators AS (
    SELECT COUNT(DISTINCT CASE WHEN experimental_strategy = 'wgs' THEN patient_id END) AS pn_to_wgs,
           COUNT(DISTINCT CASE WHEN experimental_strategy = 'wxs' THEN patient_id END) AS pn_to_wxs
    FROM occurrences
)
SELECT i.cnv_id,
       COALESCE(c.pc_to_wgs, 0)                           AS pc_to_wgs,
       d.pn_to_wgs,
       COALESCE(c.pc_to_wgs, 0) / NULLIF(d.pn_to_wgs, 0)  AS pf_to_wgs,
       COALESCE(c.pc_to_wxs, 0)                           AS pc_to_wxs,
       d.pn_to_wxs,
       COALESCE(c.pc_to_wxs, 0) / NULLIF(d.pn_to_wxs, 0)  AS pf_to_wxs
FROM intervals i
LEFT JOIN counts c ON c.cnv_id = i.cnv_id
CROSS JOIN denominators d
