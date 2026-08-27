-- Widen `snv__staging_variant.symbol` from varchar(20) to varchar(100).
--
-- THE BUG. `symbol` was declared varchar(20) on the staging table only. It is varchar(100) in the table
-- the rows come from and varchar(100) in every table they go to, so the 20 is a bottleneck in the middle
-- of a pipe that is 100 wide at both ends:
--
--   snv__tmp_variant.symbol             varchar(100)   source of snv_staging_variant_insert.sql
--   snv__staging_variant.symbol         varchar(20)    <- here
--   snv__variant.symbol                 varchar(100)   target of snv_variant_insert.sql
--   snv__variant_partitioned.symbol     varchar(100)
--
-- It has been 20 since SJRA-1252 (init/snv_staging_variant_create_table.sql) and nothing ever justified
-- it. StarRocks aborts the load rather than truncating:
--
--   Error: String 'DTX2P1-UPK3BP1-PMS2P11'(length=22) is too long. The max length of 'symbol' is 20.
--
-- THE VALUE IS CORRECT, DO NOT "CLEAN" IT UPSTREAM. `DTX2P1-UPK3BP1-PMS2P11` is a single HGNC symbol,
-- not three concatenated ones. HGNC names readthrough (conjoined) loci by joining the parent symbols
-- with hyphens -- here DTX2 pseudogene 1 / UPK3B pseudogene 1 / PMS2 pseudogene 11, a transcribed
-- pseudogene locus on 7q11.23. The rest of the failing row is coherent with that: transcript NR_023383,
-- biotype non-coding, consequences intron_variant + non_coding_transcript_variant, MODIFIER.
--
-- WHY IT ONLY SURFACES NOW. The row is a RefSeq pick (`source = 'RefSeq'`, `NR_` accession), so it
-- arrived with the VEP --merged ingestion of SJRA-1820/1826. VEP's Ensembl cache names that locus
-- differently; measured on an environment still holding mostly pre-merge data, Ensembl symbols top out
-- at 18 characters and never reached the limit.
--
-- Nothing is silently wrong in what already landed. A StarRocks INSERT is atomic, so the failed
-- statement wrote no rows at all -- there is no partially loaded or truncated symbol to repair.
--
-- Run manually, ONCE per database that holds `snv__staging_variant` (radiant/tasks/data/radiant_tables.py
-- -- STARROCKS_RADIANT_BASE_MAPPING, so the base database only; there is nothing to do in the
-- `{tenant}_tenant` databases).
--
-- New deployments get varchar(100) from init/snv_staging_variant_create_table.sql and must NOT run this
-- script. Check `DESC snv__staging_variant` first.
--
-- WHY THIS *IS* AN `ALTER TABLE ... MODIFY COLUMN`, unlike SJRA-1850. There the column was the partition
-- column and the first key column, which StarRocks refuses to modify, so it needed a rebuild-and-swap.
-- Here `symbol` is a plain value column: the PRIMARY KEY is `locus_id` alone, the distribution is
-- HASH(locus_id), and the table is not partitioned. Widening a varchar in place is the supported shape.
-- Step 2 checks that this build accepted it; if a future StarRocks version refuses, fall back to the
-- create/copy/SWAP WITH/drop sequence in SJRA-1850-snv_consequence_filter_partitioned_widen_part.sql.
--
-- `AFTER clinvar_interpretation` is required, not cosmetic. snv_staging_variant_insert.sql and
-- snv_variant_insert.sql are positional INSERTs with no target column list -- the same constraint
-- SJRA-1833 documents. Naming the position makes the statement independent of whether this StarRocks
-- version keeps a modified column in place or moves it to the end.
--
-- Cost. Varchar length is metadata; a widening MODIFY COLUMN does not rewrite or re-encode the stored
-- values and no row is longer than it was. Expect it to finish immediately. It is still an ALTER, so it
-- needs the table state to be NORMAL: schedule it outside an import_part run.


-- ---------------------------------------------------------------------------------------------------
-- 0. Confirm this database still needs the migration.
-- ---------------------------------------------------------------------------------------------------
--
--   DESC snv__staging_variant;
--       -- `symbol` must read varchar(20). If it already reads varchar(100), stop -- nothing to do.
--
--   SELECT count(*) FROM snv__staging_variant;
--       -- record it; step 3 must match. The ALTER touches no row, so any change means something else
--       -- wrote to the table concurrently.
--


-- ---------------------------------------------------------------------------------------------------
-- 1. The widening.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE snv__staging_variant
    MODIFY COLUMN symbol VARCHAR(100) NULL COMMENT '' AFTER clinvar_interpretation;


-- ---------------------------------------------------------------------------------------------------
-- 2. Let it reach FINISHED before anything else touches the table. StarRocks rejects a second ALTER
--    while the table state is not NORMAL.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN WHERE TableName = 'snv__staging_variant' ORDER BY CreateTime DESC LIMIT 1;
--       -- State must be FINISHED. CANCELLED with a "not supported" message means this build refuses
--       -- the in-place modify: use the SJRA-1850 rebuild-and-swap instead.
--


-- ---------------------------------------------------------------------------------------------------
-- 3. Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   DESC snv__staging_variant;
--       -- `symbol` varchar(100) YES, and still sitting between clinvar_interpretation and impact_score.
--       -- The column order is what the positional INSERTs depend on -- verify it, do not assume it.
--
--   SHOW CREATE TABLE snv__staging_variant;
--       -- PRIMARY KEY(locus_id), DISTRIBUTED BY HASH(locus_id) BUCKETS 10, and colocate_with still
--       -- naming <NAMESPACE>.query_group (`radiant_radiant.query_group` in QA).
--
--   SELECT count(*) FROM snv__staging_variant;
--       -- must match step 0.
--


-- ---------------------------------------------------------------------------------------------------
-- 4. Then re-run the task that failed -- `[StarRocks] Insert Staging SNV Variants` in import_part -- and
--    let the chain continue into snv__variant. snv_staging_variant_insert.sql is INSERT INTO, and it
--    wrote nothing when it aborted, so the replay is not a double-insert.
-- ---------------------------------------------------------------------------------------------------
--
--   SELECT max(char_length(symbol)) FROM snv__staging_variant;
--       -- expect > 20 once a readthrough symbol lands.
--


-- ---------------------------------------------------------------------------------------------------
-- STILL NARROW -- MEASURED, NOT FIXED HERE.
--
--   snv__consequence.symbol                      varchar(30)   PRIMARY KEY column
--   snv__consequence_filter.symbol               varchar(30)   value column
--   snv__consequence_filter_partitioned.symbol   varchar(30)   value column
--
-- These are not what failed, and widening them is not needed to unblock the import -- but the headroom
-- is 5 characters, not the comfortable margin the number suggests. Measured in QA at time of writing,
-- `snv__tmp_variant` already holds seven readthrough symbols over 20 characters across 2 245 rows:
--
--   ARHGAP27P1-BPTFP1-KPNA2P3   25      DTX2P1-UPK3BP1-PMS2P11   22
--   LINC01297-DUXAP10-NBEAP6    24      STAG3L5P-PVRIG2P-PILRB   22
--   XNDC1N-ZNF705EP-ALG1L9P     23      ANKRD20A4-ANKRD20A20P    21
--   SUGT1P4-STRA6LP-CCDC180     23
--
-- `snv__consequence` still reads max 18 only because `tg_consequences` runs after `tg_variants`
-- (import_part.py), so the run that died at the staging insert never reached it. And it is the weaker
-- bound either way: `snv__tmp_variant` carries one symbol per variant (VEP's pick), while
-- `snv__consequence` carries every transcript's, so its true maximum is >= 25.
--
-- Also fed straight from the unbounded Iceberg strings by snv_consequence_insert.sql, and narrower than
-- the same values are declared everywhere else:
--
--   snv__consequence.dna_change   varchar(1000)   vs varchar(2000) on all four snv_*variant tables
--   snv__consequence.aa_change    varchar(1000)   vs varchar(2000) on all four snv_*variant tables
--
-- `dna_change` measured 698 on a partial load. It grows with the inserted allele, so a long insertion
-- overflows the consequence table while the variant tables absorb it.
--
-- Both need their own migration: the two filter tables are DUPLICATE KEY on (locus_id, is_deleterious,
-- ...) so `symbol` is a value column and MODIFY COLUMN applies, but `snv__consequence` has
-- PRIMARY KEY(locus_id, symbol, transcript_id), so widening its `symbol` means the SJRA-1850
-- rebuild-and-swap over tens of millions of rows.
-- ---------------------------------------------------------------------------------------------------
