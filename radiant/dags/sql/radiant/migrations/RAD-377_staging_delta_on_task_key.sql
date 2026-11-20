-- RAD-377 — every case status change made the next radiant-import reload the case.
--
-- Until RAD-377, staging_external_sequencing_experiment set `updated_at` to cases.updated_on for a task with a case
-- context (the annotation tasks), and staging_sequencing_experiment_delta selected a row again when that date moved
-- (`sse.updated_at <= existing.updated_at` failed). The portal's status and priority writes bump cases.updated_on
-- (PATCH /{tenant}/cases/status, PATCH /{tenant}/cases/{case_id}); the batch writes (PUT, PATCH /cases/batch) do not.
-- The processing → in_progress move of radiant-case-scheduler put the case back in the delta, and the next import
-- reloaded its SNV and CNV annotation data (Exomiser reports included) with no data change.
--
-- Now `updated_at` is the task's created_on, and the delta anti-joins on the (case_id, seq_id, task_id) key only. A row
-- already in staging_sequencing_experiment is never selected again, so the deploy starts no mass re-import. Nothing to
-- delete or update in staging_sequencing_experiment: its rows keep their old `updated_at`, which the pending predicate
-- still compares with `ingested_at` as before.
--
-- WHERE. Base database (`USE <shared_db>`), once. Substitute the placeholder before a run:
--   <shared_db>          the base database, that holds staging_sequencing_experiment
--
-- WHEN. After the ETL deploy, with no radiant-case-scheduler, radiant-import or import_part running.


-- ---------------------------------------------------------------------------------------------------
-- 1. `CREATE VIEW IF NOT EXISTS` keeps the old definitions: drop them (the delta reads the external view, so the
--    delta goes first).
-- ---------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS staging_sequencing_experiment_delta;
DROP VIEW IF EXISTS staging_external_sequencing_experiment;


-- ---------------------------------------------------------------------------------------------------
-- 2. Run the "Radiant - Init StarRocks Base Tables" DAG with the `udf_release_version` in use: it re-creates the two
--    views. The DAG also replaces the global GET_VARIANT_ID / GET_CNV_ID UDFs from that parameter.
-- ---------------------------------------------------------------------------------------------------

--
--   SHOW CREATE VIEW staging_external_sequencing_experiment;
--       -- `updated_at` is `t.created_on`, and the view no longer joins `cases`
--
--   SHOW CREATE VIEW staging_sequencing_experiment_delta;
--       -- the LEFT ANTI JOIN has no `updated_at` condition
