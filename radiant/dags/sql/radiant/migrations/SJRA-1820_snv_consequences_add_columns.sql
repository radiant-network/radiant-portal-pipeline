-- SJRA-1820 — add every `snv__consequence` column the merged-RefSeq story needs, in one script.
--
-- Covers the three sub-tasks that touch the table's shape:
--   SJRA-1826  `source`                   — which transcript catalogue the annotation came from
--   SJRA-1827  `mane_pair_transcript_id`  — the version-free MANE cross-reference
--   SJRA-1827  `scores_from_mane_pair`    — flags the rows whose scores were borrowed through it
--
-- They are one file because they are one story against one table, and running them separately buys
-- nothing: no consequence row is loaded between them, and the ALTERs must be sequenced anyway.
--
-- Run manually, ONCE, in the base database. Unlike the SJRA-1751 migrations, `snv__consequence` is
-- NOT per-tenant: it lives in STARROCKS_RADIANT_BASE_MAPPING (radiant/tasks/data/radiant_tables.py),
-- so there is nothing to do in the `{tenant}_tenant` databases.
--
-- If an environment already ran the earlier standalone SJRA-1826 script, skip its two statements here
-- and run only the SJRA-1827 block. StarRocks has no `ADD COLUMN IF NOT EXISTS` (3.4.2), so the first
-- statement would otherwise fail — check `DESC snv__consequence` before starting.
--
-- New deployments get all three columns from init/snv_consequence_create_table.sql and must NOT run
-- this script at all.
--
-- The `AFTER` positions are required, not cosmetic: `snv_consequence_insert.sql` is a positional INSERT
-- with no target column list, so the table must end up in exactly the order declared by
-- init/snv_consequence_create_table.sql.
--
-- Issue each ALTER separately and let it reach FINISHED before the next: StarRocks rejects a second
-- ALTER while the table state is not NORMAL.
--
-- The table is NOT partitioned by source. That was proposed, measured and rejected — see §6 of
-- design/SJRA-1820-vep-merged-refseq-ingestion.md. The primary key, the distribution and the
-- colocation group are all unchanged by this migration.
--
-- HARD PREREQUISITE for the SJRA-1827 block, and it is a failure rather than a wrong result. The new
-- snv_consequence_insert.sql reads `c.mane_pair_transcript_id` and `c.transcript_id_unversioned` from
-- the *Iceberg* consequence table; SJRA-1824 added those fields to
-- radiant/tasks/vcf/snv/consequence.py::SCHEMA. `create_consequences_table()` in
-- radiant/tasks/iceberg/initialization.py drops and recreates the Iceberg table from that schema, so
-- the init-iceberg-tables DAG must have run since SJRA-1824 before the new SQL is deployed. Against an
-- older Iceberg table the insert dies on an unresolved column. CI never sees this: the test fixtures
-- build the table from the live SCHEMA.
--
-- Cost. Every ALTER TABLE below is a metadata-only light schema change and returns immediately. The one
-- UPDATE rewrites every Ensembl row (~67.3M rows / ~5.5 GB at time of writing) on a primary-key table,
-- which means minutes of write load, a new data version and persistent-index churn — schedule it in a
-- maintenance window.
--


-- ---------------------------------------------------------------------------------------------------
-- SJRA-1826 — the annotation source.
-- ---------------------------------------------------------------------------------------------------

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


-- ---------------------------------------------------------------------------------------------------
-- SJRA-1827 — the MANE cross-reference and the borrowed-scores flag.
-- ---------------------------------------------------------------------------------------------------

-- Left NULL on pre-existing rows rather than derived from `mane_select`. Deriving it would mean a
-- second ~67.3M-row UPDATE to reconstruct a value those rows do not need: they are all Ensembl, so
-- they never borrow, and `mane_select` itself is empty on every row ingested before SJRA-1822 fixed the
-- CSQ field lookup. The column fills in from the next load onward.
ALTER TABLE snv__consequence
    ADD COLUMN mane_pair_transcript_id VARCHAR(100) AFTER mane_select;

-- No backfill UPDATE here, deliberately: `DEFAULT "false"` lets the pre-existing rows read false with
-- no data rewrite, and false is factual for them — every file ingested to date was annotated against
-- the Ensembl cache only, so none of them borrowed anything.
--
-- The declaration must stay byte-identical to init/snv_consequence_create_table.sql. A nullable-there /
-- defaulted-here split would make a fresh deployment and a migrated one disagree on `IS NULL` and on
-- the schema-introspecting dbt sweeps in radiant/data_qa/sources/snv_consequence.yml.
--
-- Fallback if this turns out not to be metadata-only, or if pre-existing rows read NULL instead of the
-- default: declare it `boolean NULL` with no default in BOTH files and, only if QA insists, run
-- `UPDATE snv__consequence SET scores_from_mane_pair = FALSE WHERE scores_from_mane_pair IS NULL`.
ALTER TABLE snv__consequence
    ADD COLUMN scores_from_mane_pair BOOLEAN NOT NULL DEFAULT "false" AFTER phyloP100way_vertebrate;


-- ---------------------------------------------------------------------------------------------------
-- Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN FROM radiant;
--       -- expect State = FINISHED immediately for all three statements
--
--   DESC snv__consequence;
--       -- the column order must match init/snv_consequence_create_table.sql exactly
--
--   SELECT count(*) AS should_be_zero FROM snv__consequence WHERE scores_from_mane_pair IS NULL;
--       -- the DEFAULT must be materialised for pre-existing rows; non-zero means take the fallback
--
--   SELECT source, scores_from_mane_pair, count(*) FROM snv__consequence GROUP BY 1, 2;
--       -- expect a single row: Ensembl / false / the pre-migration row count
--
-- And after the first merged-file load, to confirm the borrow actually fired rather than silently
-- joining on nothing (the failure mode SJRA-1827 exists to avoid):
--
--   SELECT source, scores_from_mane_pair, count(*), count(sift_score), count(gnomad_pli)
--     FROM snv__consequence GROUP BY 1, 2;
--       -- no Ensembl row may carry the flag; flagged rows must include non-null sift_score AND
--       -- gnomad_pli. All-null scores on flagged rows means a versioned key reached the join.
--
--   SELECT count(*) AS should_be_zero FROM snv__consequence
--    WHERE mane_pair_transcript_id LIKE '%.%';
--       -- no version suffix may survive into the join key
