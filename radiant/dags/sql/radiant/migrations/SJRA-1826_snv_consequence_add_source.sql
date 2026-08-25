-- SJRA-1826 — add the annotation-source column to the consequence table.
--
-- Run manually, ONCE, in the base database. Unlike the SJRA-1751 migrations, `snv__consequence` is
-- NOT per-tenant: it lives in STARROCKS_RADIANT_BASE_MAPPING (radiant/tasks/data/radiant_tables.py),
-- so there is nothing to do in the `{tenant}_tenant` databases.
--
-- The `AFTER` position is required, not cosmetic: `snv_consequence_insert.sql` is a positional INSERT
-- with no target column list, so the table must end up in exactly the order declared by
-- init/snv_consequence_create_table.sql.
--
-- New deployments get this column from init/snv_consequence_create_table.sql and must NOT run this
-- script. StarRocks has no `ADD COLUMN IF NOT EXISTS` (3.4.2), so re-running this on an
-- already-migrated table fails on the first statement.
--
-- The table is NOT partitioned by source. That was proposed, measured and rejected — see §6 of
-- design/SJRA-1820-vep-merged-refseq-ingestion.md. The primary key, the distribution and the
-- colocation group are all unchanged by this migration.
--
-- Cost: the ALTER TABLE is a metadata-only light schema change and returns immediately. The UPDATE
-- rewrites every Ensembl row (~67.3M rows / ~5.5 GB at time of writing) on a primary-key table, which
-- means minutes of write load, a new data version and persistent-index churn. Run the two statements
-- separately and schedule the UPDATE in a maintenance window.
--

ALTER TABLE snv__consequence
    ADD COLUMN source VARCHAR(20) AFTER transcript_id;

-- Label the rows that were already loaded. This is factual rather than a guess: every file ingested
-- to date was annotated against the Ensembl cache, and every non-empty `transcript_id` in the table
-- is a version-free `ENST…` accession.
--
-- `source IS NULL` makes the statement safe to re-run and stops it overwriting RefSeq rows should it
-- ever be run after a merged file has landed.
--
-- Rows with no transcript at all (intergenic — empty `symbol` and empty `transcript_id`) are left
-- with a NULL source on purpose. That matches `resolve_source()` rule 4 in
-- radiant/tasks/vcf/snv/consequence.py: we record "unknown" rather than guessing a catalogue.
UPDATE snv__consequence
   SET source = 'Ensembl'
 WHERE source IS NULL
   AND transcript_id LIKE 'ENST%';
