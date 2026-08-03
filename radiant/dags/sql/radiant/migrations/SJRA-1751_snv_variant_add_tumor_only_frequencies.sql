-- SJRA-1751 — add the tumor-only somatic frequency columns to the variant catalog.
--
-- Run manually, once per database that holds `snv__variant` / `snv__variant_partitioned`. Both
-- tables are per-tenant (STARROCKS_RADIANT_PER_TENANT_MAPPING), so `USE` each `{tenant}_tenant`
-- database in turn before running this; a single-tenant deployment keeps them in the base database.
--
-- The `AFTER` positions are required, not cosmetic: `snv_variant_insert.sql` is a positional INSERT
-- with no target column list, and `snv_variant_part_insert_part.sql` copies `v.*` from
-- `snv__variant` into `snv__variant_partitioned`. Both tables must end up in exactly the order
-- declared by init/snv_variant_create_table.sql and init/snv_variant_partitioned_create_table.sql.
--
-- No migration is needed for `somatic__snv__staging_variant_frequency_part` or
-- `somatic__snv__variant_frequency`: both have carried the six `*_to_*` columns since creation.
--
-- New deployments get these columns from init/snv_variant*_create_table.sql and must NOT run this
-- script. StarRocks has no `ADD COLUMN IF NOT EXISTS` (3.4.2), so re-running this on an
-- already-migrated table fails on the first statement.
--

ALTER TABLE snv__variant
    ADD COLUMN somatic_pf_to_wgs DOUBLE AFTER somatic_pf_tn_wxs;

ALTER TABLE snv__variant
    ADD COLUMN somatic_pf_to_wxs DOUBLE AFTER somatic_pf_to_wgs;

ALTER TABLE snv__variant
    ADD COLUMN somatic_pc_to_wgs INT(11) AFTER somatic_pn_tn_wxs;

ALTER TABLE snv__variant
    ADD COLUMN somatic_pn_to_wgs INT(11) AFTER somatic_pc_to_wgs;

ALTER TABLE snv__variant
    ADD COLUMN somatic_pc_to_wxs INT(11) AFTER somatic_pn_to_wgs;

ALTER TABLE snv__variant
    ADD COLUMN somatic_pn_to_wxs INT(11) AFTER somatic_pc_to_wxs;

ALTER TABLE snv__variant_partitioned
    ADD COLUMN somatic_pf_to_wgs DOUBLE AFTER somatic_pf_tn_wxs;

ALTER TABLE snv__variant_partitioned
    ADD COLUMN somatic_pf_to_wxs DOUBLE AFTER somatic_pf_to_wgs;

ALTER TABLE snv__variant_partitioned
    ADD COLUMN somatic_pc_to_wgs INT(11) AFTER somatic_pn_tn_wxs;

ALTER TABLE snv__variant_partitioned
    ADD COLUMN somatic_pn_to_wgs INT(11) AFTER somatic_pc_to_wgs;

ALTER TABLE snv__variant_partitioned
    ADD COLUMN somatic_pc_to_wxs INT(11) AFTER somatic_pn_to_wgs;

ALTER TABLE snv__variant_partitioned
    ADD COLUMN somatic_pn_to_wxs INT(11) AFTER somatic_pc_to_wxs;
