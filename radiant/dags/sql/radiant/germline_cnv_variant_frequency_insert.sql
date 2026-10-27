-- RAD-120: in-cohort frequency of each distinct germline CNV interval (`cnv_id`) of the tenant. Two CNVs match
-- when they are on the same chromosome, in the same class, and their overlap covers at least 0.8 of each one's
-- length. A part cannot be counted alone (a patient of one part can match an interval of another), so this is a
-- full recompute over every part, run per tenant. Design: design/RAD-120-cnv-cohort-frequency.md.
INSERT OVERWRITE {{ mapping.starrocks_germline_cnv_variant_frequency }}
WITH
-- Every occurrence of the tenant, flagged instead of filtered (RAD-57): an interval whose occurrences all fail
-- the gate keeps a row with zero counts, and its patients stay in the denominators.
occurrences AS (
    SELECT o.cnv_id,
           o.filter = 'PASS' AS qualifies,
           s.patient_id,
           s.experimental_strategy,
           s.affected_status
    FROM {{ mapping.starrocks_germline_cnv_occurrence }} o
    JOIN {{ mapping.starrocks_staging_sequencing_experiment }} s ON s.seq_id = o.seq_id
    WHERE s.analysis_type = 'germline'
      AND s.tenant_code = %(tenant_code)s
),
-- One row per distinct interval. ANY_VALUE is safe: `cnv_id` packs chromosome, start, end and type.
-- GAIN and GAINLOH are pooled (copy number up in both), as in the gnomAD-SV join of the occurrence insert.
intervals AS (
    SELECT cnv_id,
           ANY_VALUE(chromosome) AS chromosome,
           ANY_VALUE(`start`)    AS `start`,
           ANY_VALUE(`end`)      AS `end`,
           CASE WHEN ANY_VALUE(`type`) IN ('GAIN', 'GAINLOH') THEN 'GAIN' ELSE ANY_VALUE(`type`) END AS match_class
    FROM {{ mapping.starrocks_germline_cnv_occurrence }}
    GROUP BY cnv_id
),
-- Length bucket (power of 2) and position bin (bin width = the bucket's length). Matching lengths are within
-- 1.25x, so in the same or an adjacent bucket; a matching start is less than one target bin away.
binned AS (
    SELECT cnv_id, chromosome, `start`, `end`, match_class,
           FLOOR(LOG2(`end` - `start`))                              AS len_bucket,
           FLOOR(`start` / POW(2, FLOOR(LOG2(`end` - `start`))))    AS pos_bin
    FROM intervals
    WHERE `end` > `start`
),
-- Nine probe keys per interval: bucket +-1, and the bin recomputed with the TARGET bucket's width, +-1. Equality
-- keys keep the self-join a hash join; a range predicate degrades to a nested loop.
probe AS (
    SELECT d.cnv_id, d.chromosome, d.match_class, d.`start`, d.`end`,
           d.len_bucket + b.db                                     AS t_len_bucket,
           FLOOR(d.`start` / POW(2, d.len_bucket + b.db)) + p.dp   AS t_pos_bin
    FROM binned d
    CROSS JOIN (SELECT -1 AS db UNION ALL SELECT 0 UNION ALL SELECT 1) b
    CROSS JOIN (SELECT -1 AS dp UNION ALL SELECT 0 UNION ALL SELECT 1) p
),
-- 0.8 = reciprocal overlap threshold. The +-1 bins are lossless only for a threshold >= 0.75: below it, matches
-- are dropped silently (widen to +-2 first).
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
    SELECT DISTINCT cnv_id, patient_id, experimental_strategy, affected_status
    FROM occurrences
    WHERE qualifies
),
-- COUNT(DISTINCT patient_id): a patient carrying several segments that match the interval counts once.
counts AS (
    SELECT m.query_id AS cnv_id,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wgs' THEN c.patient_id END) AS pc_wgs,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wgs' AND c.affected_status = 'affected' THEN c.patient_id END) AS pc_wgs_affected,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wgs' AND c.affected_status = 'non_affected' THEN c.patient_id END) AS pc_wgs_not_affected,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wxs' THEN c.patient_id END) AS pc_wxs,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wxs' AND c.affected_status = 'affected' THEN c.patient_id END) AS pc_wxs_affected,
           COUNT(DISTINCT CASE WHEN c.experimental_strategy = 'wxs' AND c.affected_status = 'non_affected' THEN c.patient_id END) AS pc_wxs_not_affected
    FROM matches m
    JOIN carriers c ON c.cnv_id = m.match_id
    GROUP BY m.query_id
),
-- Patients with at least one CNV occurrence, whatever its filter.
denominators AS (
    SELECT COUNT(DISTINCT CASE WHEN experimental_strategy = 'wgs' THEN patient_id END) AS pn_wgs,
           COUNT(DISTINCT CASE WHEN experimental_strategy = 'wgs' AND affected_status = 'affected' THEN patient_id END) AS pn_wgs_affected,
           COUNT(DISTINCT CASE WHEN experimental_strategy = 'wgs' AND affected_status = 'non_affected' THEN patient_id END) AS pn_wgs_not_affected,
           COUNT(DISTINCT CASE WHEN experimental_strategy = 'wxs' THEN patient_id END) AS pn_wxs,
           COUNT(DISTINCT CASE WHEN experimental_strategy = 'wxs' AND affected_status = 'affected' THEN patient_id END) AS pn_wxs_affected,
           COUNT(DISTINCT CASE WHEN experimental_strategy = 'wxs' AND affected_status = 'non_affected' THEN patient_id END) AS pn_wxs_not_affected
    FROM occurrences
),
-- Every interval gets a row, also when no qualifying CNV matches it (or when it has no length to bin).
frequencies AS (
    SELECT i.cnv_id,
           COALESCE(c.pc_wgs, 0)              AS pc_wgs,
           COALESCE(c.pc_wgs_affected, 0)     AS pc_wgs_affected,
           COALESCE(c.pc_wgs_not_affected, 0) AS pc_wgs_not_affected,
           COALESCE(c.pc_wxs, 0)              AS pc_wxs,
           COALESCE(c.pc_wxs_affected, 0)     AS pc_wxs_affected,
           COALESCE(c.pc_wxs_not_affected, 0) AS pc_wxs_not_affected
    FROM intervals i
    LEFT JOIN counts c ON c.cnv_id = i.cnv_id
)
SELECT f.cnv_id,
       f.pc_wgs,
       d.pn_wgs,
       f.pc_wgs / NULLIF(d.pn_wgs, 0)                           AS pf_wgs,
       f.pc_wgs_affected,
       d.pn_wgs_affected,
       f.pc_wgs_affected / NULLIF(d.pn_wgs_affected, 0)         AS pf_wgs_affected,
       f.pc_wgs_not_affected,
       d.pn_wgs_not_affected,
       f.pc_wgs_not_affected / NULLIF(d.pn_wgs_not_affected, 0) AS pf_wgs_not_affected,
       f.pc_wxs,
       d.pn_wxs,
       f.pc_wxs / NULLIF(d.pn_wxs, 0)                           AS pf_wxs,
       f.pc_wxs_affected,
       d.pn_wxs_affected,
       f.pc_wxs_affected / NULLIF(d.pn_wxs_affected, 0)         AS pf_wxs_affected,
       f.pc_wxs_not_affected,
       d.pn_wxs_not_affected,
       f.pc_wxs_not_affected / NULLIF(d.pn_wxs_not_affected, 0) AS pf_wxs_not_affected
FROM frequencies f
CROSS JOIN denominators d
