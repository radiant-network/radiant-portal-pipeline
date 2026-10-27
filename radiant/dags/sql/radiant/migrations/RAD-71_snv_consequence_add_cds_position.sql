-- RAD-71 — the CDS position of each consequence, split out of VEP's `CDS_position`.
--
--   cds_start   INT   first base of the variant on the coding sequence (for an insertion, the base before it)
--   cds_end     INT   last base of the variant on the coding sequence (for an insertion, the base after it)
--
-- A single position (`123`, an SNV) fills both columns, a range (`123-125`) fills one each, and a range with
-- one end outside the CDS (`?-125`, `123-?`) leaves that end NULL. Both are NULL outside the CDS. Nothing
-- reads them yet: the ACMG classifier (RAD-70) will.
--
-- Run manually, ONCE, in the base database. `snv__consequence` is NOT per-tenant: it lives in
-- STARROCKS_RADIANT_BASE_MAPPING (radiant/tasks/data/radiant_tables.py), so there is nothing to do in the
-- `{tenant}_tenant` databases.
--
-- New deployments get the columns from init/snv_consequence_create_table.sql and must NOT run this script.
-- StarRocks has no `ADD COLUMN IF NOT EXISTS` (3.4.2), so re-running it fails on the first ALTER.
--
-- Altered in place rather than dropped and recreated: the init DDL is `CREATE TABLE IF NOT EXISTS`, and a
-- full reload of a production takes 1 to 2 weeks. Each ALTER is a metadata-only light schema change, and the
-- existing rows read NULL.
--
-- The `AFTER` positions are required, not cosmetic: snv_consequence_insert.sql and
-- snv_consequence_reannotate.sql are positional INSERTs with no target column list, so the table must end up
-- in exactly the order declared by init/snv_consequence_create_table.sql. `cds_end` is added after
-- `cds_start`, so the two statements must run in this order.
--
-- Issue each ALTER separately and let it reach FINISHED before the next: StarRocks rejects a second ALTER
-- while the table state is not NORMAL.
--
-- Order of the rollout, in each environment:
--
--   1. Run the Init Iceberg Tables DAG by hand, with no import running. The Iceberg writer takes its schema
--      from the live table, so against the old `snv_consequence` Iceberg table the new values are dropped and
--      snv_consequence_insert.sql then fails on the missing `cds_start`.
--   2. Run this script, and deploy the new DAGs right after, with no import_part or reannotate_open_data
--      running. Between the two, an old insert fails against the new table and a new insert against the
--      old one.
--   3. Re-import every experiment. That is the backfill: until a locus is re-imported, its consequences
--      keep NULL in both columns.
--
-- Data QA during the backfill: snv_consequence__should_not_contain_only_null skips cds_start and cds_end, and
-- snv_consequence__validate_cds_position_on_coding_terms_RAD-71 reports every coding consequence not yet
-- re-imported as a known error, which does not fail the run. Once every environment is re-imported, remove
-- both columns from that `except:` and the `_RAD-71` suffix, so the two tests guard the columns.


ALTER TABLE snv__consequence
    ADD COLUMN cds_start INT NULL COMMENT "CDS position of the first base of the variant (VEP CDS_position). For an insertion, the base before the insertion point. NULL outside the CDS." AFTER dna_change;

ALTER TABLE snv__consequence
    ADD COLUMN cds_end INT NULL COMMENT "CDS position of the last base of the variant (VEP CDS_position). For an insertion, the base after the insertion point. NULL outside the CDS." AFTER cds_start;


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED for both statements
--
--   DESC snv__consequence;
--       -- cds_start and cds_end must be the last two columns, after dna_change, in that order
--
-- After the re-import:
--
--   SELECT count(*) AS should_be_non_zero FROM snv__consequence WHERE cds_start IS NOT NULL;
--       -- 0 means the re-import did not run, or ran against the old Iceberg table
--
--   SELECT count(*) AS should_be_zero FROM snv__consequence WHERE cds_start > cds_end;
