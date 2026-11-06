-- The proband's slivar CNV set: parental origin, transmission mode and the parents' calls.
--
--   parental_origin     VARCHAR(25)           the proband's `po_*` tag, with the germline SNV values (DENOVO, MOTHER...)
--   transmission_mode   VARCHAR(50)           the CNV mode tag that lists the proband, under its own name (denovo...)
--   mother_calls        ARRAY<INT>            the mother's GT of the same slivar record, as `calls`
--   mother_cn           INT                   her CN
--   mother_bc           INT                   her BC
--   mother_pe           ARRAY<INT>            her PE
--   mother_sm           FLOAT                 her SM
--   mother_name         VARCHAR(1048576)      her own DRAGEN call ID (FORMAT/OID), equal to `name` on her alignment-task row
--   father_*                                  the same six columns for the father
--
-- The columns are filled on the proband's rows of a `radiant_germline_cnv_annotation` task only. The rows of an
-- `alignment_germline_variant_calling` task keep them NULL. On the annotation-task rows, `name` is the proband's own
-- DRAGEN call ID (FORMAT/OID), equal to `name` on its alignment-task row, not the slivar record's ID (the call
-- truvari kept for the cluster, often a parent's). A slivar VCF with no OID keeps the record's ID.
--
-- Run manually, ONCE in every {tenant}_tenant database (germline__cnv__occurrence is per tenant,
-- STARROCKS_RADIANT_PER_TENANT_MAPPING in radiant/tasks/data/radiant_tables.py). `USE` each database in turn.
-- A single-tenant deployment keeps the table in the base database.
--
-- The columns are appended at the end on purpose: the occurrence insert is a positional INSERT with no target
-- column list, and germline_cnv_occurrence_copy_partition.sql copies `SELECT *`. The table must end up in exactly
-- the order declared by init/germline_cnv_occurrence_create_table.sql.
--
-- New deployments get the columns from init/germline_cnv_occurrence_create_table.sql and must NOT run this script.
-- StarRocks has no `ADD COLUMN IF NOT EXISTS`, so re-running it fails on the ALTER.
--
-- HARD PREREQUISITES for the deployment, BEFORE the new DAGs run, with no import running
-- (radiant-hub docs/features/RAD-169-cnv-post-processing.md, "Merge and rollout order"):
--   1. this script, in every {tenant}_tenant database. Against an unmigrated table the positional insert fails and
--      every CNV import breaks;
--   2. the Iceberg `germline_cnv_occurrence` table recreated with the new schema: run ONLY the germline CNV
--      occurrence task of the init-iceberg-tables DAG (the other tasks drop and recreate their own tables). Each
--      import overwrites this table, so nothing is lost. The schema also makes the elements of `pe`, `mother_pe`
--      and `father_pe` nullable (a partly missing `PE` vector): a table created from an earlier revision of this
--      branch has to be recreated again, or the parquet writer rejects the first such vector and the whole part's
--      germline CNV load fails;
--   3. the two staging views dropped (base database), then `radiant-init-starrocks-base-tables` run: the views are
--      created with `IF NOT EXISTS`. Otherwise the slivar VCFs stay invisible, and the proband source rule does not
--      apply:
--
--        DROP VIEW staging_sequencing_experiment_delta;
--        DROP VIEW staging_external_sequencing_experiment;
--
-- Let the ALTER reach FINISHED in a database before the next import runs there.
--
-- No backfill: the DRAGEN rows keep the new columns NULL, which is their final value. The next `radiant-import`
-- loads every slivar VCF already registered: their staging rows are new, so the delta view selects them.


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE germline__cnv__occurrence ADD COLUMN (
    parental_origin VARCHAR(25) NULL COMMENT "",
    transmission_mode VARCHAR(50) NULL COMMENT "",
    mother_calls ARRAY<INT> NULL COMMENT "",
    mother_cn INT NULL COMMENT "",
    mother_bc INT NULL COMMENT "",
    mother_pe ARRAY<INT> NULL COMMENT "",
    mother_sm FLOAT NULL COMMENT "",
    mother_name VARCHAR(1048576) NULL COMMENT "",
    father_calls ARRAY<INT> NULL COMMENT "",
    father_cn INT NULL COMMENT "",
    father_bc INT NULL COMMENT "",
    father_pe ARRAY<INT> NULL COMMENT "",
    father_sm FLOAT NULL COMMENT "",
    father_name VARCHAR(1048576) NULL COMMENT ""
);


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED
--
--   DESC germline__cnv__occurrence;
--       -- the fourteen columns must be the last ones, after gnomad_ac (RAD-194). RAD-194 adds gnomad_ac
--       -- `AFTER gnomad_sc_het`, so the two migrations give this order in either order.
--
-- After the next import of a case with a slivar VCF:
--
--   SELECT transmission_mode, parental_origin, count(*)
--   FROM germline__cnv__occurrence
--   WHERE task_id IN (SELECT DISTINCT task_id FROM <shared_db>.staging_sequencing_experiment
--                     WHERE task_type = 'radiant_germline_cnv_annotation')
--   GROUP BY 1, 2;
