-- RAD-288 — the journal names the task a Nextflow pipeline registered for each item it ran.
--
-- pipeline_journal gets the column `output_task_id`, and pipeline_state shows it. Only where the journal already
-- exists (an environment that deployed it from main): the Init StarRocks Base Tables DAG creates a new journal with
-- the column. The rows already written keep NULL.
--
-- WHERE. Base database (`USE <shared_db>`), once. Substitute the placeholder before a run:
--   <shared_db>          the base database, that holds pipeline_journal
--
-- WHEN. After the ETL deploy, with no radiant-case-scheduler, radiant-import or "from Cases" DAG running: their
-- journal writes fail until step 1 has added the column.


-- ---------------------------------------------------------------------------------------------------
-- 1. Add the column, last: the insert names its columns.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE pipeline_journal ADD COLUMN output_task_id INT NULL
    COMMENT "On a succeeded row of a Nextflow pipeline: the task it registered for the item; NULL otherwise";


-- ---------------------------------------------------------------------------------------------------
-- 2. `CREATE VIEW IF NOT EXISTS` keeps the old pipeline_state: drop it, then run the "Radiant - Init StarRocks Base
--    Tables" DAG with the `udf_release_version` in use, which creates it again with the column.
-- ---------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS pipeline_state;

--
--   SHOW CREATE VIEW pipeline_state;
--       -- the SELECT lists output_task_id
--


-- ---------------------------------------------------------------------------------------------------
-- Rollback. Deploy the previous ETL version; the extra column is harmless to it. To remove it anyway:
-- ---------------------------------------------------------------------------------------------------

-- ALTER TABLE pipeline_journal DROP COLUMN output_task_id;
-- DROP VIEW IF EXISTS pipeline_state;   -- then run the Init StarRocks Base Tables DAG
