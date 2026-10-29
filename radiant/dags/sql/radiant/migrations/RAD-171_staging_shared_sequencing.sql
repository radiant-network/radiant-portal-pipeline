-- RAD-171 — an experiment shared by two cases got the annotation tasks of both cases.
--
-- Until RAD-171, staging_external_sequencing_experiment joined task_context on the experiment only: every case of a
-- shared experiment got a row for every task over it, its own and the other case's ("cross rows"), and the family
-- join on the patient only could give a row another case's family_id / family_role. The import builds a task from
-- its rows without the case (import_part.py), so a cross row adds the experiment to the task a second time, maybe
-- with another family_role or in another part.
--
-- The fixed view no longer emits the cross rows, but the delta only adds and updates: the cross rows already in
-- staging_sequencing_experiment stay until deleted here.
--
-- WHERE. Base database (`USE <shared_db>`), once. Substitute the placeholders before a run:
--   <shared_db>          the base database, that holds staging_sequencing_experiment
--   <clinical_catalog>   the JDBC catalog of the clinical database, and <clinical_db> its database: the prefix that
--   <clinical_db>        `mapping.clinical_task_context` renders to (radiant/tasks/data/radiant_tables.py)
--
-- WHEN. After the ETL deploy, with no radiant-import / import_part running.
--
-- Rows of a task whose task_context names no case (alignment_germline_variant_calling, ...) are shared by design
-- and never touched here.


-- ---------------------------------------------------------------------------------------------------
-- 0a. Cross rows, read-only. Save the result: steps 2 and 3 use it. Empty means steps 2 and 3 have nothing to do.
-- ---------------------------------------------------------------------------------------------------
--
-- A staging row whose task names its cases in task_context, none of them the row's case.

SELECT s.case_id, s.seq_id, s.task_id, s.task_type, s.part, s.family_role, s.ingested_at
FROM staging_sequencing_experiment s
JOIN (
    SELECT DISTINCT task_id
    FROM <clinical_catalog>.<clinical_db>.task_context
    WHERE case_id IS NOT NULL
) owned ON owned.task_id = s.task_id
LEFT JOIN (
    SELECT DISTINCT task_id, case_id
    FROM <clinical_catalog>.<clinical_db>.task_context
    WHERE case_id IS NOT NULL
) tc ON tc.task_id = s.task_id AND tc.case_id = s.case_id
WHERE tc.task_id IS NULL
ORDER BY s.task_id, s.case_id, s.seq_id;


-- ---------------------------------------------------------------------------------------------------
-- 0b. Orphan parts, read-only. Must return nothing, or clean up by hand.
-- ---------------------------------------------------------------------------------------------------
--
-- A cross row in a part where its task has no row of its own case. The occurrences that the import wrote there
-- under (seq_id, task_id) belong to no remaining staging row: the re-import of step 3 rewrites the task's own part
-- only. Delete them from that part in the occurrence tables of the tenant.

SELECT x.task_id, x.part
FROM (
    SELECT s.task_id, s.part, MAX(tc.task_id IS NOT NULL) AS has_own_row
    FROM staging_sequencing_experiment s
    LEFT JOIN (
        SELECT DISTINCT task_id, case_id
        FROM <clinical_catalog>.<clinical_db>.task_context
        WHERE case_id IS NOT NULL
    ) tc ON tc.task_id = s.task_id AND tc.case_id = s.case_id
    WHERE s.task_id IN (
        SELECT DISTINCT task_id FROM <clinical_catalog>.<clinical_db>.task_context WHERE case_id IS NOT NULL
    )
    GROUP BY s.task_id, s.part
) x
WHERE NOT x.has_own_row;


-- ---------------------------------------------------------------------------------------------------
-- 1. The fixed views. `CREATE VIEW IF NOT EXISTS` keeps the old definition: drop both, then run the Init StarRocks
--    Tables DAG, which re-creates them.
-- ---------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS staging_sequencing_experiment_delta;
DROP VIEW IF EXISTS staging_external_sequencing_experiment;

--
--   SHOW CREATE VIEW staging_external_sequencing_experiment;
--       -- the task_context join carries `tctx.case_id IS NULL OR tctx.case_id = se.case_id`
--


-- ---------------------------------------------------------------------------------------------------
-- 2. Delete the cross rows of check 0a. `DELETE ... USING` needs a Primary Key table (StarRocks 3.0 or later).
-- ---------------------------------------------------------------------------------------------------

DELETE FROM staging_sequencing_experiment
USING (
    SELECT s.case_id, s.seq_id, s.task_id
    FROM staging_sequencing_experiment s
    JOIN (
        SELECT DISTINCT task_id
        FROM <clinical_catalog>.<clinical_db>.task_context
        WHERE case_id IS NOT NULL
    ) owned ON owned.task_id = s.task_id
    LEFT JOIN (
        SELECT DISTINCT task_id, case_id
        FROM <clinical_catalog>.<clinical_db>.task_context
        WHERE case_id IS NOT NULL
    ) tc ON tc.task_id = s.task_id AND tc.case_id = s.case_id
    WHERE tc.task_id IS NULL
) x
WHERE staging_sequencing_experiment.case_id = x.case_id
  AND staging_sequencing_experiment.seq_id = x.seq_id
  AND staging_sequencing_experiment.task_id = x.task_id;

--
--   Re-run check 0a: it returns nothing.
--


-- ---------------------------------------------------------------------------------------------------
-- 3. Re-import marks. Every row of the tasks of check 0a: their occurrences may hold the experiment twice or
--    another case's family_role. `radiant-import` then picks them (`updated_at >= COALESCE(ingested_at, ...)`) and
--    rebuilds each task from its own rows. Replace <task_ids> with the distinct task_id of check 0a.
-- ---------------------------------------------------------------------------------------------------

UPDATE staging_sequencing_experiment
SET ingested_at = NULL
WHERE task_id IN (<task_ids>);
