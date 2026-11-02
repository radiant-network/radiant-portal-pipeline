-- RAD-194 (RAD-163 task 1) — gnomAD SV allele count on the CNV occurrences.
--
--   gnomad_ac   INT(11)   `ac` of the gnomAD SV call (`gnomad_sv_v1`) the CNV matches: same chromosome, DEL / DUP
--                         from the CNV type, 80% reciprocal overlap, best call by AF then SF. NULL when nothing
--                         matches, like gnomad_af, gnomad_sc and gnomad_sf.
--
-- *_cnv_occurrence_insert_partition_delta.sql and *_cnv_occurrence_reannotate_partition.sql fill it from the
-- gnomAD SV join they already do.
--
-- Run manually, ONCE in every {tenant}_tenant database (both tables are per tenant,
-- STARROCKS_RADIANT_PER_TENANT_MAPPING in radiant/tasks/data/radiant_tables.py). `USE` each database in turn.
-- A single-tenant deployment keeps the tables in the base database.
--
-- The column is appended at the end on purpose: the occurrence inserts are positional INSERTs with no target
-- column list, and *_cnv_occurrence_copy_partition.sql copies `SELECT *`. Both tables must end up in exactly
-- the order declared by init/*_cnv_occurrence_create_table.sql.
--
-- New deployments get the column from init/*_cnv_occurrence_create_table.sql and must NOT run this script.
-- StarRocks has no `ADD COLUMN IF NOT EXISTS`, so re-running it fails on the ALTER.
--
-- HARD PREREQUISITE for the deployment: run this BEFORE the new DAGs are deployed. Against an unmigrated
-- table the positional inserts fail and every import_part with CNVs breaks.
--
-- Issue each ALTER separately and let it reach FINISHED before the next: StarRocks rejects a second ALTER
-- while the table state is not NORMAL. No table is dropped.
--
-- Backfill: the existing occurrences keep a NULL gnomad_ac until the CNV branch of reannotate_open_data
-- rebuilds their partitions. Its gates watch the open-data sources, not the code, so trigger it once per
-- environment with `force_reannotation`, at a time when no import is expected (it holds the import lock).


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE germline__cnv__occurrence ADD COLUMN gnomad_ac INT(11) NULL COMMENT "" AFTER gnomad_sc_het;

ALTER TABLE somatic__cnv__occurrence ADD COLUMN gnomad_ac INT(11) NULL COMMENT "" AFTER gnomad_sc_het;


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED for both statements
--
--   DESC germline__cnv__occurrence;
--   DESC somatic__cnv__occurrence;
--       -- gnomad_ac must be the last column, after gnomad_sc_het.
--
-- After the forced re-annotation:
--
--   SELECT count(*) AS should_be_zero FROM germline__cnv__occurrence
--    WHERE (gnomad_ac IS NULL) <> (gnomad_af IS NULL);
--       -- AC is set exactly when a gnomAD SV call matched. Same check on somatic__cnv__occurrence.
--
--   SELECT count(*) AS should_be_non_zero FROM germline__cnv__occurrence WHERE gnomad_ac IS NOT NULL;
--       -- 0 means the re-annotation did not run.
