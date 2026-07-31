-- SJRA-1751 — add FORMAT/SQ, INFO/AQ and the DRAGEN hotspot flag to the somatic occurrence table.
--
-- Run manually, once per database that holds a `somatic__snv__occurrence` table. The table is
-- per-tenant (STARROCKS_RADIANT_PER_TENANT_MAPPING), so `USE` each `{tenant}_tenant` database
-- in turn before running this; a single-tenant deployment keeps it in the base database.
--
-- New deployments get these columns from `init/somatic_snv_occurrence_create_table.sql` and
-- must NOT run this script. StarRocks has no `ADD COLUMN IF NOT EXISTS` (3.4.2), so
-- re-running this on an already-migrated table fails on the first statement.
--

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN info_hotspot BOOLEAN AFTER info_hotspotallele;

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN info_aq FLOAT AFTER info_mapq;

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN tumor_sq FLOAT AFTER tumor_gt_status;

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN normal_sq FLOAT AFTER normal_gt_status;
