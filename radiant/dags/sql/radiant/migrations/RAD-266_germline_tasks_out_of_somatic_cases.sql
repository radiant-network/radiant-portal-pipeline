-- RAD-266 — a germline task got a row in the somatic case of its experiment.
--
-- Until RAD-266, staging_external_sequencing_experiment matched the germline task types (alignment_germline_variant_calling,
-- radiant_germline_annotation, radiant_germline_cnv_annotation) to every case of the experiment. The normal of a
-- tumor-normal case is often in a germline case too: its alignment task (no case in task_context) got a `germline`
-- row and a `somatic` row. sequencing_experiment_partition_select.sql drops case_id, but its DISTINCT cannot merge
-- two rows that differ by analysis_type, so import_part built the task from two rows and failed the whole part
-- ("`alignment_germline_variant_calling` task expects a single row per task").
--
-- The fixed view no longer emits these rows, but the delta only adds and updates: the rows already in
-- staging_sequencing_experiment stay until deleted here.
--
-- WHERE. Base database (`USE <shared_db>`), once. Substitute the placeholder before a run:
--   <shared_db>          the base database, that holds staging_sequencing_experiment
--
-- WHEN. After the ETL deploy, with no radiant-import / import_part running.


-- ---------------------------------------------------------------------------------------------------
-- 0. The somatic rows of germline tasks, read-only. Empty means step 2 has nothing to do.
--    `germline_parts` is the part of the same task's germline rows: a different part (or none) means the task was
--    imported there under the somatic row alone, and its occurrences in that part need a look before step 2.
-- ---------------------------------------------------------------------------------------------------

SELECT s.case_id, s.seq_id, s.task_id, s.task_type, s.part, s.deleted, s.ingested_at, g.germline_parts
FROM staging_sequencing_experiment s
LEFT JOIN (
    SELECT task_id, GROUP_CONCAT(DISTINCT CAST(part AS VARCHAR)) AS germline_parts
    FROM staging_sequencing_experiment
    WHERE analysis_type = 'germline'
    GROUP BY task_id
) g ON g.task_id = s.task_id
WHERE s.analysis_type = 'somatic'
  AND s.task_type IN ('alignment_germline_variant_calling', 'radiant_germline_annotation', 'radiant_germline_cnv_annotation')
ORDER BY s.task_id, s.case_id;


-- ---------------------------------------------------------------------------------------------------
-- 1. The fixed views. `CREATE VIEW IF NOT EXISTS` keeps the old definition: drop both, then run the Init StarRocks
--    Tables DAG, which re-creates them.
-- ---------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS staging_sequencing_experiment_delta;
DROP VIEW IF EXISTS staging_external_sequencing_experiment;

--
--   SHOW CREATE VIEW staging_external_sequencing_experiment;
--       -- the sequencing_experiments CTE carries `NOT (se.analysis_type = 'somatic' AND t.task_type_code IN (...))`
--


-- ---------------------------------------------------------------------------------------------------
-- 2. Delete the rows of check 0.
-- ---------------------------------------------------------------------------------------------------

DELETE FROM staging_sequencing_experiment
WHERE analysis_type = 'somatic'
  AND task_type IN ('alignment_germline_variant_calling', 'radiant_germline_annotation', 'radiant_germline_cnv_annotation');

--
--   Re-run check 0: it returns nothing. The next radiant-import loads the parts that failed.
--
