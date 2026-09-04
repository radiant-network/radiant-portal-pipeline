-- SJRA-1833 — record which transcript catalogue VEP's picked consequence came from.
--
-- `snv__variant` carries a headline block copied verbatim from the picked consequence: transcript_id,
-- symbol, hgvsg, hgvsc, hgvsp, dna_change, aa_change, is_canonical, is_mane_select, is_mane_plus,
-- consequences, vep_impact, impact_score. With VEP `--merged` that pick is a RefSeq transcript for
-- ~1.4% of variants and nothing in the row says so. `pick_source` says so. The pick itself is
-- unchanged — see §5 of design/SJRA-1820-vep-merged-refseq-ingestion.md for why overriding it would
-- break the block's internal coherence.
--
-- Run manually, ONCE per database. Unlike the SJRA-1820 consequence migration, this one spans both
-- scopes (radiant/tasks/data/radiant_tables.py):
--
--   base database           snv__tmp_variant, snv__staging_variant   (STARROCKS_RADIANT_BASE_MAPPING)
--   every {tenant}_tenant   snv__variant, snv__variant_partitioned   (STARROCKS_RADIANT_PER_TENANT_MAPPING)
--
-- `USE` each database in turn and run only its block. A single-tenant deployment keeps all four in the
-- base database.
--
-- The `AFTER transcript_id` positions are required, not cosmetic: snv_tmp_variant_insert.sql,
-- snv_staging_variant_insert.sql and snv_variant_insert.sql are positional INSERTs with no target
-- column list, and snv_variant_part_insert_part.sql copies `v.*` from `snv__variant` into
-- `snv__variant_partitioned`. All four tables must end up in exactly the order declared by their
-- init/*_create_table.sql.
--
-- New deployments get the column from init/snv_*_variant*_create_table.sql and must NOT run this
-- script. StarRocks has no `ADD COLUMN IF NOT EXISTS` (3.4.2), so re-running it fails on the ALTER.
--
-- Issue each ALTER separately and let it reach FINISHED before the next: StarRocks rejects a second
-- ALTER while the table state is not NORMAL.
--
-- HARD PREREQUISITE. snv_tmp_variant_insert.sql now reads `t.pick_source` from the *Iceberg* variant
-- table; SJRA-1833 added that field to radiant/tasks/vcf/snv/variant.py::SCHEMA.
-- `create_variant_table()` in radiant/tasks/iceberg/initialization.py drops and recreates the Iceberg
-- table from that schema, so the init-iceberg-tables DAG must have run before the new SQL is deployed.
-- Against an older Iceberg table the insert dies on an unresolved column. CI never sees this: the test
-- fixtures build the table from the live SCHEMA.
--
-- Cost. Every ALTER is a metadata-only light schema change and returns immediately. The two UPDATEs
-- rewrite every Ensembl row on primary-key tables, which means write load, a new data version and
-- persistent-index churn — schedule them in a maintenance window.


-- ---------------------------------------------------------------------------------------------------
-- Base database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE snv__tmp_variant
    ADD COLUMN pick_source VARCHAR(20) AFTER transcript_id;

-- No backfill on snv__tmp_variant: it is INSERT OVERWRITE-d from Iceberg on every import and holds
-- nothing worth preserving between runs.

ALTER TABLE snv__staging_variant
    ADD COLUMN pick_source VARCHAR(20) AFTER transcript_id;

-- snv__staging_variant is INSERT INTO, so it accumulates across imports and is what snv__variant is
-- rebuilt from. Labelling it here is what makes the backfill stick past the next import.
--
-- Factual rather than a guess: every file ingested to date was annotated against the Ensembl cache.
-- `pick_source IS NULL` makes the statement re-runnable and stops it overwriting RefSeq rows should it
-- ever run after a merged file has landed. Variants whose picked consequence had no transcript keep a
-- NULL pick_source on purpose — that matches `resolve_source()` rule 4 in
-- radiant/tasks/vcf/snv/consequence.py: we record "unknown" rather than guessing a catalogue.
UPDATE snv__staging_variant
   SET pick_source = 'Ensembl'
 WHERE pick_source IS NULL
   AND transcript_id LIKE 'ENST%';


-- ---------------------------------------------------------------------------------------------------
-- Every {tenant}_tenant database.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE snv__variant
    ADD COLUMN pick_source VARCHAR(20) AFTER transcript_id;

UPDATE snv__variant
   SET pick_source = 'Ensembl'
 WHERE pick_source IS NULL
   AND transcript_id LIKE 'ENST%';

ALTER TABLE snv__variant_partitioned
    ADD COLUMN pick_source VARCHAR(20) AFTER transcript_id;

-- No UPDATE here, and not an oversight: snv__variant_partitioned declares no key clause, so it is a
-- Duplicate Key table and StarRocks restricts UPDATE to Primary Key tables. The column fills in per
-- part on the next import_part run, which dynamic_overwrite-s that part from the now-backfilled
-- snv__variant. If it must be populated everywhere sooner, re-run snv_variant_part_insert_part.sql for
-- each existing `part` value.


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM <database>;
--       -- expect State = FINISHED immediately for every statement
--
--   DESC snv__variant;
--       -- pick_source must sit between transcript_id and omim_inheritance_code, matching
--       -- init/snv_variant_create_table.sql. Same check on the other three tables.
--
--   SELECT pick_source, count(*) FROM snv__variant GROUP BY 1;
--       -- expect Ensembl plus, possibly, a NULL bucket for transcript-less picks. No RefSeq yet.
--
-- And after the first merged-file load:
--
--   SELECT pick_source, count(*) FROM snv__variant GROUP BY 1;
--       -- expect a RefSeq slice near 1.4%. Zero means the source never reached the picked consequence.
--
--   SELECT count(*) AS should_be_zero FROM snv__variant
--    WHERE (pick_source = 'RefSeq' AND transcript_id LIKE 'ENST%')
--       OR (pick_source = 'Ensembl' AND transcript_id NOT LIKE 'ENST%' AND transcript_id <> '');
--       -- pick_source and the headline transcript must agree; a mismatch means the block was mixed
