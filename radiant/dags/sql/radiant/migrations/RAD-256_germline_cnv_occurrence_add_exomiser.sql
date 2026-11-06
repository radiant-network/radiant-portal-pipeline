-- The CNV Exomiser result on the proband's slivar CNV set (RAD-181).
--
--   exomiser_rank                  INT                 the `#RANK` column of the CNV Exomiser `variants.tsv`
--   exomiser_variant_score         FLOAT               `EXOMISER_VARIANT_SCORE`
--   exomiser_gene_combined_score   FLOAT               `EXOMISER_GENE_COMBINED_SCORE`
--   exomiser_moi                   VARCHAR(10)         `MOI`
--   exomiser_acmg_classification   VARCHAR(300)        `EXOMISER_ACMG_CLASSIFICATION`, lower case as for SNVs
--   exomiser_acmg_evidence         ARRAY<VARCHAR(10)>  `EXOMISER_ACMG_EVIDENCE`, split on `,`
--
-- Named and typed as on germline__snv__occurrence, plus the rank. Filled on the rows of a
-- `radiant_germline_cnv_annotation` task only, from the row of the report with the highest gene combined score for
-- the record. The rows of an `alignment_germline_variant_calling` task (the DRAGEN set) keep them NULL.
--
-- Every contributing row of the report also goes to a new per-tenant table, `exomiser_cnv` (the twin of `exomiser`
-- for SNVs): `insert_exomiser_cnv` matches the report's `VCF_ID` to the occurrence's `name`, else to the new Iceberg
-- column `vcf_id` (the VCF record ID), and ranks the rows of each occurrence (`variant_rank`). cnv-post-processing runs
-- Exomiser on the proband VCF (record ID = the proband's own call, `name`) when it has one, else on the family VCF
-- (record ID = the call truvari kept, `vcf_id`, which on the slivar set can differ from `name`). The occurrence insert
-- copies the `variant_rank = 1` row. The StarRocks occurrence table does not carry `vcf_id`: the re-annotation reads
-- the Exomiser columns back.
--
-- `exomiser_cnv` is created by section 1, in every tenant database. `radiant-import` also creates the per-tenant
-- tables (`prepare_tenants_tables`, CREATE TABLE IF NOT EXISTS), but only for the tenants of its own delta: a tenant
-- with no new data would have no `exomiser_cnv` until its next import, and the data_qa tenant passes, which test
-- every tenant, would fail on the missing table.
--
-- Design: radiant-hub docs/features/RAD-181-*.md. Deviations from the plan: the join falls back to `vcf_id`, a new
-- column of the Iceberg germline_cnv_occurrence table (step 2 below), and the per-tenant `exomiser_cnv` table.
--
-- WHEN. Once per environment, BEFORE the new DAGs run, with no import running. In this order:
--
--   0. The radiant-operator image of this release deployed (the image `import_part` runs the VCF extraction in).
--      The extraction builds its Arrow table from the Iceberg table's schema (radiant/tasks/vcf/cnv/germline/
--      process.py) and drops, without an error, any key the schema does not have. An older image never writes
--      `vcf_id`: against the new table the column stays NULL, and the records of a report run on the family VCF get
--      no Exomiser value, with no failure. Check the image tag of the ECS task definition / K8s operator first.
--   1. Every {tenant}_tenant database: section 1 (the ALTER, and the `exomiser_cnv` table). Against an unmigrated
--      table the positional insert fails and every CNV import breaks. Let the ALTER reach FINISHED before the next
--      import runs there.
--   2. The Iceberg `germline_cnv_occurrence` table recreated with the new schema (`vcf_id`): run ONLY the germline
--      CNV occurrence task of the init-iceberg-tables DAG (the other tasks drop and recreate their own tables). Each
--      import overwrites this table, so nothing is lost. Otherwise `insert_exomiser_cnv` fails on `o.vcf_id`.
--   3. Base database: drop the two staging views, then run `radiant-init-starrocks-base-tables` with the
--      `udf_release_version` in use. It creates `raw_exomiser_cnv` and the views with the new definition (both
--      `IF NOT EXISTS`). Otherwise the CNV reports stay invisible and the load has no table:
--
--        DROP VIEW staging_sequencing_experiment_delta;
--        DROP VIEW staging_external_sequencing_experiment;
--
--   4. Pause `radiant-case-scheduler` (see LAB EMAILS below). Then base database: section 2 (backfill), after
--      step 3 (it reads the new view).
--   5. Trigger `radiant-import` manually. It re-imports the marked parts and loads the reports. Wait until it and
--      every `radiant-import-part` run it triggers succeed, check that the pending query of section 2 returns 0, then
--      unpause `radiant-case-scheduler`.
--
-- LAB EMAILS. Section 2b makes every marked case pending for the import (import_pending_case_select.sql). If
-- `radiant-case-scheduler` runs while they are pending, it counts them as cases with work, triggers the import, and
-- then emails the laboratory of every one of them that is past `processing` (`in_progress`, `in_review`,
-- `completed`...): `processed_case_ids` in radiant/tasks/nextflow/case_status.py. `radiant-import` has no schedule
-- of its own, so without step 5 the case scheduler is the run that imports them. Keep it paused from 2b until the
-- manual import has finished. Skip the pause only if the laboratories must get an email for these old cases.
--
-- WHERE. Section 1 runs in EVERY {tenant}_tenant database (germline__cnv__occurrence is per tenant,
-- STARROCKS_RADIANT_PER_TENANT_MAPPING in radiant/tasks/data/radiant_tables.py). `USE` each database in turn. A
-- single-tenant deployment keeps the table in the base database. Section 2 runs ONCE in the base database
-- (`USE <shared_db>`).
--
-- The columns are appended at the end on purpose: the occurrence insert is a positional INSERT with no target
-- column list, and germline_cnv_occurrence_copy_partition.sql copies `SELECT *`. The table must end up in exactly
-- the order declared by init/germline_cnv_occurrence_create_table.sql.
--
-- New deployments get the columns from init/germline_cnv_occurrence_create_table.sql and must NOT run the ALTER of
-- section 1. StarRocks has no `ADD COLUMN IF NOT EXISTS`, so re-running it fails on the ALTER. The CREATE TABLE of
-- section 1 is `IF NOT EXISTS`: safe to re-run.


-- ---------------------------------------------------------------------------------------------------
-- 1. Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE germline__cnv__occurrence ADD COLUMN (
    exomiser_rank INT NULL COMMENT "",
    exomiser_variant_score FLOAT NULL COMMENT "",
    exomiser_gene_combined_score FLOAT NULL COMMENT "",
    exomiser_moi VARCHAR(10) NULL COMMENT "",
    exomiser_acmg_classification VARCHAR(300) NULL COMMENT "",
    exomiser_acmg_evidence ARRAY<VARCHAR(10)> NULL COMMENT ""
);

--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED
--
--   DESC germline__cnv__occurrence;
--       -- the six columns must be the last ones, after father_name (RAD-206)
--

-- The tenant `exomiser_cnv` table. Same definition as init/exomiser_cnv_create_table.sql: keep the two in sync.

CREATE TABLE IF NOT EXISTS exomiser_cnv
(
    part                 INT,
    seq_id               INT,
    task_id              INT NOT NULL,
    cnv_id               BIGINT,
    id                   VARCHAR(2000),
    name                 VARCHAR(1048576),
    vcf_id               VARCHAR(2000),
    moi                  VARCHAR(10),
    variant_score        FLOAT,
    gene_combined_score  FLOAT,
    variant_rank         INT,
    rank                 INT,
    symbol               VARCHAR(200),
    acmg_classification  VARCHAR(300),
    acmg_evidence        ARRAY<VARCHAR(10)>
)
ENGINE = OLAP
DUPLICATE KEY(`part`, `seq_id`, `task_id`, `cnv_id`, `id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`seq_id`) BUCKETS 10;

--
--   DESC exomiser_cnv;
--       -- variant_rank must be INT: a table created by an earlier revision of this branch has TINYINT. Drop it
--       -- (it is empty until the first import of this release) and run the CREATE TABLE again.
--


-- ---------------------------------------------------------------------------------------------------
-- 2. Backfill, base database, after the views are re-created (step 3). The CNV annotation tasks already loaded.
-- ---------------------------------------------------------------------------------------------------
--
-- 2a. The proband's `radiant_germline_cnv_annotation` row gets its file from the view, on the same key. The delta view
--     selects only new or updated rows, so without 2b the existing cases get no Exomiser value until their next case
--     change.
--
-- `UPDATE ... FROM` needs StarRocks 3.0 or later (staging_sequencing_experiment is a Primary Key table). When several
-- rows of the join match one target row, StarRocks uses one at random: check 2.0 makes the match unique.

-- 2.0 Check, read-only. Must return nothing. A proband CNV annotation row that the view gives several files.

SELECT case_id, seq_id, task_id, COUNT(DISTINCT exomiser_filepath) AS exomiser_files
FROM staging_external_sequencing_experiment
WHERE task_type = 'radiant_germline_cnv_annotation'
  AND exomiser_filepath IS NOT NULL
GROUP BY case_id, seq_id, task_id
HAVING COUNT(DISTINCT exomiser_filepath) > 1;

-- 2a.

UPDATE staging_sequencing_experiment
SET exomiser_filepath = v.exomiser_filepath
FROM (
    SELECT DISTINCT case_id, seq_id, task_id, exomiser_filepath
    FROM staging_external_sequencing_experiment
    WHERE task_type = 'radiant_germline_cnv_annotation'
      AND exomiser_filepath IS NOT NULL
) v
WHERE staging_sequencing_experiment.case_id = v.case_id
  AND staging_sequencing_experiment.seq_id = v.seq_id
  AND staging_sequencing_experiment.task_id = v.task_id
  AND staging_sequencing_experiment.task_type = 'radiant_germline_cnv_annotation'
  AND staging_sequencing_experiment.family_role = 'proband'
  AND NOT staging_sequencing_experiment.deleted;

-- 2b. Re-import marks: every staging row of these tasks, for every family member. A part builds each task only from
--     the rows its select returns, and the slivar set needs the parents' rows for the `mother_*` / `father_*` columns.
--     `radiant-import` then picks them (`updated_at >= COALESCE(ingested_at, ...)`). Run it with
--     `radiant-case-scheduler` paused, and trigger `radiant-import` manually right after (WHEN, steps 4 and 5; LAB
--     EMAILS).

UPDATE staging_sequencing_experiment
SET ingested_at = NULL
FROM (
    SELECT DISTINCT task_id
    FROM staging_sequencing_experiment
    WHERE task_type = 'radiant_germline_cnv_annotation'
      AND family_role = 'proband'
      AND exomiser_filepath IS NOT NULL
      AND NOT deleted
) marked
WHERE staging_sequencing_experiment.task_id = marked.task_id;

--
--   SELECT count(*) FROM staging_sequencing_experiment
--   WHERE task_type = 'radiant_germline_cnv_annotation' AND family_role = 'proband' AND exomiser_filepath IS NOT NULL;
--       -- the number of CNV annotation tasks with an Exomiser report
--
-- Pending query, before unpausing `radiant-case-scheduler` (WHEN, step 5):
--
--   SELECT count(*) FROM staging_sequencing_experiment
--   WHERE task_type = 'radiant_germline_cnv_annotation'
--     AND (updated_at >= COALESCE(ingested_at, '1970-01-01 00:00:00') OR deleted);
--       -- 0 once the manual import has loaded every marked task
--
-- After the next import, in each {tenant}_tenant database:
--
--   SELECT exomiser_acmg_classification, count(*)
--   FROM germline__cnv__occurrence
--   WHERE task_id IN (SELECT DISTINCT task_id FROM <shared_db>.staging_sequencing_experiment
--                     WHERE task_type = 'radiant_germline_cnv_annotation')
--   GROUP BY 1;
--       -- some rows with a classification
--
--   SELECT count(*)
--   FROM germline__cnv__occurrence
--   WHERE task_id IN (SELECT DISTINCT task_id FROM <shared_db>.staging_sequencing_experiment
--                     WHERE task_type = 'alignment_germline_variant_calling')
--     AND exomiser_acmg_classification IS NOT NULL;
--       -- 0
--
--   SELECT count(*), count(DISTINCT task_id), sum(variant_rank = 1)
--   FROM exomiser_cnv;
--       -- every contributing row of the reports; one `variant_rank = 1` row per ranked occurrence, as many as the
--       -- germline__cnv__occurrence rows with an exomiser_rank
