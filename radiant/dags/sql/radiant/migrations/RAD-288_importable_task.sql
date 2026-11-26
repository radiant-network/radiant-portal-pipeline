-- RAD-288 — the import decides on its facts, in Python, and journals its decisions.
--
-- staging_external_sequencing_experiment is renamed importable_task. staging_sequencing_experiment_delta is gone: its
-- query is now part of import_facts_select.sql. staging_sequencing_experiment is unchanged, nothing to reload.
--
-- WHERE. Base database (`USE <shared_db>`), once. Substitute the placeholder before a run:
--   <shared_db>          the base database, that holds staging_sequencing_experiment
--
-- WHEN. After the ETL deploy, with no radiant-case-scheduler, radiant-import or import_part running: the import and
-- the case scheduler's discovery fail until step 2 has created importable_task.


-- ---------------------------------------------------------------------------------------------------
-- 1. Drop the two old views (the delta reads the external view, so the delta goes first).
-- ---------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS staging_sequencing_experiment_delta;
DROP VIEW IF EXISTS staging_external_sequencing_experiment;


-- ---------------------------------------------------------------------------------------------------
-- 2. Run the "Radiant - Init StarRocks Base Tables" DAG with the `udf_release_version` in use: it creates
--    importable_task. The DAG also replaces the global GET_VARIANT_ID / GET_CNV_ID UDFs from that parameter.
-- ---------------------------------------------------------------------------------------------------

--
--   SELECT COUNT(*) FROM importable_task;
--       -- above zero on an environment with cases
--


-- ---------------------------------------------------------------------------------------------------
-- Rollback. Drop importable_task, deploy the previous ETL version, run the Init StarRocks Base Tables DAG.
-- ---------------------------------------------------------------------------------------------------

-- DROP VIEW IF EXISTS importable_task;
