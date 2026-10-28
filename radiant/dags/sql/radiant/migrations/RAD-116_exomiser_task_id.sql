-- The Exomiser files become outputs of the annotation tasks, and the Exomiser tables are keyed per task.
--
--   staging_sequencing_experiment   the `exomiser_filepath` of each legacy `exomiser` row moves to the proband's
--                                   `radiant_germline_annotation` row of the same case and sequencing; the legacy rows
--                                   are deleted
--   raw_exomiser                    `task_id INT` added after `acmg_evidence`, the last ordinary column (the generated
--                                   columns `locus` and `locus_hash` stay last), outside the key. No backfill
--   exomiser                        rebuilt with `task_id INT NOT NULL` as its third column, key
--                                   (part, seq_id, task_id, locus_id, id), then swapped in
--
-- Design: radiant-hub docs/features/RAD-116-embed-exomiser-in-annotation-tasks.md (appendix B for the rebuild).
--
-- WHEN. Once per environment, inside the data freeze: after the PostgreSQL migration of the portal and after the ETL
-- deploy (with the staging views dropped and re-created by the init DAG), and BEFORE `radiant-import` resumes. The
-- import marks as deleted every staging row whose task is no longer in the clinical DB; the next part import would
-- then drop the proband's Exomiser rows. Once step 1 has deleted the legacy rows, nothing is left to mark.
--
-- STEPS. Numbered as the spec: checks 0a to 0d ("Check first"), then steps 1 to 4 of the StarRocks migration. Step 3
-- is the rebuild of appendix B: its sections are 3.B0 to 3.B7, after the appendix B step numbers. Appendix B step 6
-- (re-import marks) is spec step 4, so it runs as section 4, after step 3 is done in every tenant database; 3.B7 is
-- for the end of the freeze.
--
-- WHERE. Checks 0a, 0b, 0d and steps 1, 2 and 4 run ONCE in the base database (`USE <shared_db>`). Check 0c and step 3
-- run in EVERY {tenant}_tenant database (`USE` each in turn; exomiser is per tenant,
-- STARROCKS_RADIANT_PER_TENANT_MAPPING in radiant/tasks/data/radiant_tables.py). A single-tenant deployment keeps
-- `exomiser` in the base database.
--
-- Substitute the placeholders before a run:
--   <shared_db>   the base database, that holds staging_sequencing_experiment and raw_exomiser
--   <tenant>      the tenant code of the {tenant}_tenant database in use
--   <NAMESPACE>   the Airflow namespace: `colocate_with` is `{{ mapping.colocate_query_group }}`, that is
--                 `<NAMESPACE>.query_group` (STARROCKS_COLOCATE_GROUP_MAPPING)
--
-- Run checks 0a to 0c in every database BEFORE any write. If one returns a row, stop and fix the data.
--
-- `UPDATE ... FROM` needs StarRocks 3.0 or later (staging_sequencing_experiment is a Primary Key table). When several
-- rows of the join match one target row, StarRocks uses one at random: checks 0a and 0b make the match unique.
--
-- New deployments get the new columns from init/staging_exomiser_create_table.sql and init/exomiser_create_table.sql
-- and must NOT run this script. StarRocks has no `ADD COLUMN IF NOT EXISTS`, so re-running step 2 fails on the ALTER.


-- ---------------------------------------------------------------------------------------------------
-- 0a. Check, base database, read-only. Must return nothing.
-- ---------------------------------------------------------------------------------------------------
--
-- A legacy `exomiser` row with no proband annotation row of the same case and sequencing, or with several: step 1
-- would not move its file, or would move it to a random one of them.

SELECT l.case_id, l.seq_id, l.task_id, COUNT(a.task_id) AS proband_annotation_rows
FROM staging_sequencing_experiment l
LEFT JOIN staging_sequencing_experiment a
    ON a.case_id = l.case_id
   AND a.seq_id = l.seq_id
   AND a.task_type = 'radiant_germline_annotation'
   AND a.family_role = 'proband'
   AND NOT a.deleted
WHERE l.task_type = 'exomiser'
  AND NOT l.deleted
GROUP BY l.case_id, l.seq_id, l.task_id
HAVING COUNT(a.task_id) <> 1;


-- ---------------------------------------------------------------------------------------------------
-- 0b. Check, base database, read-only. Must return nothing.
-- ---------------------------------------------------------------------------------------------------
--
-- A proband annotation row that several legacy `exomiser` rows of the same case and sequencing match: step 1 would
-- keep one of their files at random and lose the others.

SELECT a.case_id, a.seq_id, a.task_id, COUNT(l.task_id) AS legacy_rows
FROM staging_sequencing_experiment a
JOIN staging_sequencing_experiment l
    ON l.case_id = a.case_id
   AND l.seq_id = a.seq_id
   AND l.task_type = 'exomiser'
   AND NOT l.deleted
WHERE a.task_type = 'radiant_germline_annotation'
  AND a.family_role = 'proband'
  AND NOT a.deleted
GROUP BY a.case_id, a.seq_id, a.task_id
HAVING COUNT(l.task_id) > 1;


-- ---------------------------------------------------------------------------------------------------
-- 0c. Check, every {tenant}_tenant database, read-only. Must return nothing.
-- ---------------------------------------------------------------------------------------------------
--
-- An `exomiser` row whose sequencing will have no proband annotation row with an `exomiser_filepath` once step 1 is
-- done: no task can own it. With checks 0a and 0b clean, every legacy row gives its sequencing such a row.

SELECT e.seq_id, COUNT(1) AS exomiser_rows
FROM exomiser e
LEFT JOIN (
    SELECT DISTINCT seq_id
    FROM <shared_db>.staging_sequencing_experiment
    WHERE tenant_code = '<tenant>'
      AND NOT deleted
      AND (
          task_type = 'exomiser'
          OR (task_type = 'radiant_germline_annotation' AND family_role = 'proband' AND exomiser_filepath IS NOT NULL)
      )
) owned ON owned.seq_id = e.seq_id
WHERE owned.seq_id IS NULL
GROUP BY e.seq_id;


-- ---------------------------------------------------------------------------------------------------
-- 0d. Re-run sequencings, base database, read-only. Save the list: 3.B3, step 4 and checklist step 8 use it.
-- ---------------------------------------------------------------------------------------------------
--
-- A sequencing with several proband annotation tasks carrying an Exomiser file once step 1 is done (a duo, then a
-- trio). Its `exomiser` rows cannot tell which file they come from: 3.B2 leaves them out, step 4 re-imports them.

SELECT a.seq_id, COUNT(DISTINCT a.task_id) AS exomiser_annotation_tasks
FROM staging_sequencing_experiment a
LEFT JOIN (
    SELECT DISTINCT case_id, seq_id
    FROM staging_sequencing_experiment
    WHERE task_type = 'exomiser'
      AND NOT deleted
) l ON l.case_id = a.case_id AND l.seq_id = a.seq_id
WHERE a.task_type = 'radiant_germline_annotation'
  AND a.family_role = 'proband'
  AND NOT a.deleted
  AND (a.exomiser_filepath IS NOT NULL OR l.seq_id IS NOT NULL)
GROUP BY a.seq_id
HAVING COUNT(DISTINCT a.task_id) > 1
ORDER BY a.seq_id;


-- ---------------------------------------------------------------------------------------------------
-- 1. Staging rows, base database. The file path moves to the proband's annotation row, then the legacy rows go.
--    `updated_at` and `ingested_at` stay as they are, so the import does not pick the annotation rows again.
-- ---------------------------------------------------------------------------------------------------

UPDATE staging_sequencing_experiment
SET exomiser_filepath = legacy.exomiser_filepath
FROM (
    SELECT case_id, seq_id, exomiser_filepath
    FROM staging_sequencing_experiment
    WHERE task_type = 'exomiser'
      AND NOT deleted
) legacy
WHERE staging_sequencing_experiment.case_id = legacy.case_id
  AND staging_sequencing_experiment.seq_id = legacy.seq_id
  AND staging_sequencing_experiment.task_type = 'radiant_germline_annotation'
  AND staging_sequencing_experiment.family_role = 'proband'
  AND NOT staging_sequencing_experiment.deleted;

DELETE FROM staging_sequencing_experiment
WHERE task_type = 'exomiser';

--
--   SELECT count(*) FROM staging_sequencing_experiment WHERE task_type = 'exomiser';
--       -- 0
--   SELECT count(*) FROM staging_sequencing_experiment
--   WHERE task_type = 'radiant_germline_annotation' AND family_role = 'proband' AND exomiser_filepath IS NOT NULL;
--       -- the number of `exomiser` rows counted before the freeze (checklist step 2)
--


-- ---------------------------------------------------------------------------------------------------
-- 2. raw_exomiser, base database. No backfill: the old rows keep a NULL task_id. The insert never reads them (it
--    selects the run's tasks), and the next copy step of their part drops them (`NULL NOT IN ...` is never true).
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE raw_exomiser ADD COLUMN task_id INT NULL COMMENT "" AFTER acmg_evidence;

--
--   SHOW ALTER TABLE COLUMN FROM <shared_db>;
--       -- expect State = FINISHED before step 4
--   DESC raw_exomiser;
--       -- task_id right after acmg_evidence, before locus and locus_hash
--


-- ---------------------------------------------------------------------------------------------------
-- 3.B0. Counts before, every {tenant}_tenant database, read-only. 3.B3 and 3.B5 compare against them.
-- ---------------------------------------------------------------------------------------------------
--
--   SELECT part, seq_id, count(*) FROM exomiser GROUP BY part, seq_id ORDER BY part, seq_id;
--


-- ---------------------------------------------------------------------------------------------------
-- 3.B1. The replacement table, every {tenant}_tenant database. Identical to init/exomiser_create_table.sql except for
--    the name. Same partition column, distribution, bucket count and colocation group: otherwise the swap takes the
--    table out of the colocation group that the occurrence joins use.
-- ---------------------------------------------------------------------------------------------------

CREATE TABLE exomiser_task_id
(
    part                 INT,
    seq_id               INT,
    task_id              INT NOT NULL,
    locus_id             BIGINT,
    id                   VARCHAR(2000),
    locus_hash           VARCHAR(256),
    moi                  VARCHAR(10),
    variant_score        FLOAT,
    gene_combined_score  FLOAT,
    variant_rank         TINYINT,
    rank                 INT,
    symbol               VARCHAR(200),
    acmg_classification  VARCHAR(300),
    acmg_evidence array< VARCHAR (10)>
)
ENGINE = OLAP
DUPLICATE KEY(`part`, `seq_id`, `task_id`, `locus_id`, `id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "<NAMESPACE>.query_group"
);


-- ---------------------------------------------------------------------------------------------------
-- 3.B2. Copy with task_id, every {tenant}_tenant database. A sequencing with exactly one proband annotation task
--    carrying an Exomiser file gives its rows that task id. The inner join leaves out, on purpose, the re-run
--    sequencings of check 0d (re-imported in step 4) and the rows no task can own (stopped by check 0c).
--    The insert is positional: check the column list against `DESC exomiser` first.
-- ---------------------------------------------------------------------------------------------------

INSERT INTO exomiser_task_id
SELECT e.part, e.seq_id, m.task_id, e.locus_id, e.id, e.locus_hash, e.moi,
       e.variant_score, e.gene_combined_score, e.variant_rank, e.rank, e.symbol,
       e.acmg_classification, e.acmg_evidence
FROM exomiser e
JOIN (
    SELECT seq_id, MIN(task_id) AS task_id
    FROM <shared_db>.staging_sequencing_experiment
    WHERE task_type = 'radiant_germline_annotation'
      AND family_role = 'proband'
      AND exomiser_filepath IS NOT NULL
      AND NOT deleted
      AND tenant_code = '<tenant>'
    GROUP BY seq_id
    HAVING COUNT(DISTINCT task_id) = 1
) m ON m.seq_id = e.seq_id;


-- ---------------------------------------------------------------------------------------------------
-- 3.B3. Checks before the swap, read-only, while undoing is free.
-- ---------------------------------------------------------------------------------------------------
--
--   SELECT part, seq_id, count(*) FROM exomiser_task_id GROUP BY part, seq_id ORDER BY part, seq_id;
--       -- equal to 3.B0 for every kept sequencing; no row for the re-run sequencings of check 0d
--
--   SELECT count(*) FROM exomiser_task_id WHERE task_id IS NULL;
--       -- 0
--
--   DESC exomiser_task_id;
--   SHOW CREATE TABLE exomiser_task_id;
--       -- task_id third, DUPLICATE KEY(part, seq_id, task_id, locus_id, id)
--


-- ---------------------------------------------------------------------------------------------------
-- 3.B4. Atomic name exchange, every {tenant}_tenant database. After this `exomiser` holds the new column and
--    `exomiser_task_id` holds the old table.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE exomiser SWAP WITH exomiser_task_id;


-- ---------------------------------------------------------------------------------------------------
-- 3.B5. Checks after the swap, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW CREATE TABLE exomiser;
--       -- DUPLICATE KEY(part, seq_id, task_id, locus_id, id), BUCKETS 10, colocate_with <NAMESPACE>.query_group
--
--   SELECT part, seq_id, count(*) FROM exomiser GROUP BY part, seq_id ORDER BY part, seq_id;
--       -- the counts of 3.B3
--
--   SHOW PROC '/colocation_group';
--       -- the group must be stable, not in an unhealthy state
--


-- ---------------------------------------------------------------------------------------------------
-- 4. Re-import marks (appendix B step 6), base database, once step 3 is done in every tenant database. Every staging
--    row of the annotation tasks of the re-run sequencings (check 0d), for every family member: a part builds each
--    task only from the rows its select returns. `radiant-import` then picks them
--    (`updated_at >= COALESCE(ingested_at, ...)`) and loads each task's own file (checklist step 8).
-- ---------------------------------------------------------------------------------------------------

UPDATE staging_sequencing_experiment
SET ingested_at = NULL
FROM (
    SELECT DISTINCT task_id
    FROM staging_sequencing_experiment
    WHERE task_type = 'radiant_germline_annotation'
      AND family_role = 'proband'
      AND exomiser_filepath IS NOT NULL
      AND NOT deleted
      AND seq_id IN (
          SELECT seq_id
          FROM staging_sequencing_experiment
          WHERE task_type = 'radiant_germline_annotation'
            AND family_role = 'proband'
            AND exomiser_filepath IS NOT NULL
            AND NOT deleted
          GROUP BY seq_id
          HAVING COUNT(DISTINCT task_id) > 1
      )
) rerun
WHERE staging_sequencing_experiment.task_id = rerun.task_id;


-- ---------------------------------------------------------------------------------------------------
-- 3.B7. Old table. Keep it until the end of the freeze (checklist step 9): a rollback is then a second swap, with no
--     restore. Then, in every {tenant}_tenant database:
-- ---------------------------------------------------------------------------------------------------
--
--   DROP TABLE exomiser_task_id;
--
