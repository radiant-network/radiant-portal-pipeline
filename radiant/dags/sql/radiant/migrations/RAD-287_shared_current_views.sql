-- RAD-287 — the case filter and the supersession policy move into four shared views.
--
-- New views, created by the Init StarRocks Base Tables DAG: pipeline_case, current_experiment, current_alignment,
-- case_member. The pending queries read them at run time; staging_external_sequencing_experiment reads pipeline_case and
-- case_member, so it must be re-created after them. No behavior change: every query returns the same rows. Nothing to
-- delete from staging_sequencing_experiment.
--
-- WHERE. Base database (`USE <shared_db>`), once. Substitute the placeholder before a run:
--   <shared_db>          the base database, that holds staging_sequencing_experiment
--
-- WHEN. After the ETL deploy, with no radiant-case-scheduler, radiant-import or import_part running: the pending
-- queries and case_status_select fail until step 2 has created the views.


-- ---------------------------------------------------------------------------------------------------
-- 1. `CREATE VIEW IF NOT EXISTS` keeps the old staging definition: drop it (the delta reads the external view, so the
--    delta goes first). The old definition returns the same rows, so skipping this step breaks nothing, but the
--    deployed staging view would then not read pipeline_case and would drift from the next change to it. What must not
--    be skipped is step 2: the pending queries and case_status_select read the new views.
-- ---------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS staging_sequencing_experiment_delta;
DROP VIEW IF EXISTS staging_external_sequencing_experiment;


-- ---------------------------------------------------------------------------------------------------
-- 2. Run the "Radiant - Init StarRocks Base Tables" DAG: it creates the four views, then re-creates the two staging
--    views on top of them.
-- ---------------------------------------------------------------------------------------------------

--
--   SHOW CREATE VIEW staging_external_sequencing_experiment;
--       -- the sequencing_context CTE joins pipeline_case, the final SELECT joins case_member
--
--   SELECT COUNT(*) FROM pipeline_case;
--   SELECT COUNT(*) FROM current_experiment;
--   SELECT COUNT(*) FROM current_alignment;
--   SELECT COUNT(*) FROM case_member;
--       -- every count is above zero on an environment with cases
--


-- ---------------------------------------------------------------------------------------------------
-- Rollback. Drop the six views, deploy the previous ETL version, run the Init StarRocks Base Tables DAG.
-- ---------------------------------------------------------------------------------------------------

-- DROP VIEW IF EXISTS staging_sequencing_experiment_delta;
-- DROP VIEW IF EXISTS staging_external_sequencing_experiment;
-- DROP VIEW IF EXISTS case_member;
-- DROP VIEW IF EXISTS current_alignment;
-- DROP VIEW IF EXISTS current_experiment;
-- DROP VIEW IF EXISTS pipeline_case;
