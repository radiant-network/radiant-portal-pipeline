-- Widen `snv__consequence_filter_partitioned.part` from tinyint(4) to INT.
--
-- THE BUG. `part` was declared tinyint(4) — max 127 — while every other partitioned table declares it
-- INT (germline/somatic __snv__occurrence, germline/somatic __cnv__occurrence, snv__variant_partitioned,
-- the staging frequency tables, exomiser). Part numbers are not small: SequencingTypeMask in
-- radiant/tasks/starrocks/partition.py seeds WGS at 0x00000000 and WXS at 0x00010000, so every WXS part
-- is >= 65536. `snv_consequence_filter_insert_part.sql` selects `%(part)s AS part`, StarRocks casts the
-- out-of-range literal to NULL rather than raising, and the load then dies on the NOT NULL constraint:
--
--   Error: NULL value in non-nullable column 'part'. Row: [NULL, -3169129651586990080, 0, 1, ...]
--
-- It fails only on WXS. WGS parts count up from 0 and stay under 127 until part 128, i.e. 12 800
-- experiments, so a WGS-only deployment loads clean and hides the defect. Nothing is silently wrong in
-- the rows that did land: the load aborts, so no part was ever written under a truncated number.
--
-- Run manually, ONCE per database that holds `snv__consequence_filter_partitioned`
-- (radiant/tasks/data/radiant_tables.py — STARROCKS_RADIANT_BASE_MAPPING, so the base database only;
-- there is nothing to do in the `{tenant}_tenant` databases).
--
-- New deployments get INT from init/snv_consequence_filter_partitioned_create_table.sql and must NOT run
-- this script. Check `DESC snv__consequence_filter_partitioned` first.
--
-- WHY THIS IS NOT AN `ALTER TABLE ... MODIFY COLUMN`. `part` is the partition column AND the first
-- column of the inferred DUPLICATE KEY. StarRocks refuses MODIFY COLUMN on either, so the type change
-- has to be a rebuild-and-swap. Verified against StarRocks 4.0.11.
--
-- Cost. One full copy of the table (~44.5M rows in QA at time of writing, a few minutes) plus an atomic
-- metadata swap. The swap is what keeps readers consistent: no window in which the table is missing or
-- half-populated. Schedule it outside an import_part run — a concurrent
-- `INSERT OVERWRITE ... dynamic_overwrite` against the old table would be lost by the swap.
--
-- The copy is only there to avoid recomputing. If losing the existing parts is acceptable, skip step 2
-- and re-run import_part (or just snv_consequence_filter_insert_part.sql) once per existing `part`
-- instead — the table is fully derivable from `snv__consequence_filter` plus the occurrence tables.


-- ---------------------------------------------------------------------------------------------------
-- 0. Record what has to survive, so step 5 has something to check against.
-- ---------------------------------------------------------------------------------------------------
--
--   SELECT part, count(*) FROM snv__consequence_filter_partitioned GROUP BY part ORDER BY part;
--


-- ---------------------------------------------------------------------------------------------------
-- 1. The replacement table. Byte-identical to
--    init/snv_consequence_filter_partitioned_create_table.sql except for the name — same column order,
--    same partition column, same distribution and bucket count, same colocation group. All three have
--    to match or the swap leaves the table out of the `query_group` colocation it is queried under.
--
--    `{{ mapping.colocate_query_group }}` resolves to `<NAMESPACE>.query_group`
--    (STARROCKS_COLOCATE_GROUP_MAPPING); substitute the literal for your deployment — `radiant_radiant`
--    is the QA namespace.
-- ---------------------------------------------------------------------------------------------------

CREATE TABLE snv__consequence_filter_partitioned_int_part (
  `part` INT NOT NULL COMMENT "",
  `locus_id` bigint(20) NULL COMMENT "",
  `is_deleterious` boolean NOT NULL COMMENT "",
  `impact_score` tinyint(4) NULL COMMENT "",
  `symbol` varchar(30) NULL COMMENT "",
  `consequence` varchar(50) NULL COMMENT "",
  `biotype` varchar(50) NULL COMMENT "",
  `spliceai_ds` float NULL COMMENT "",
  `sift_score` float NULL COMMENT "",
  `sift_pred` varchar(1) NULL COMMENT "",
  `polyphen2_hvar_score` float NULL COMMENT "",
  `polyphen2_hvar_pred` varchar(1) NULL COMMENT "",
  `fathmm_score` float NULL COMMENT "",
  `fathmm_pred` varchar(1) NULL COMMENT "",
  `cadd_score` float NULL COMMENT "",
  `cadd_phred` float NULL COMMENT "",
  `dann_score` float NULL COMMENT "",
  `revel_score` float NULL COMMENT "",
  `lrt_score` float NULL COMMENT "",
  `lrt_pred` varchar(1) NULL COMMENT "",
  `gnomad_pli` float NULL COMMENT "",
  `gnomad_loeuf` float NULL COMMENT "",
  `phyloP17way_primate` float NULL COMMENT "",
  `phyloP100way_vertebrate` float NULL COMMENT "",
  `vep_impact` VARCHAR(20) NULL COMMENT ""
)
ENGINE=OLAP
COMMENT "OLAP"
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES ("colocate_with" = "radiant_radiant.query_group");


-- ---------------------------------------------------------------------------------------------------
-- 2. Carry the existing parts over. Positional `SELECT *` is deliberate and safe here: both tables are
--    in the column order the init DDL declares, which is also what
--    snv_consequence_filter_insert_part.sql relies on. Widening tinyint -> INT never loses a value.
-- ---------------------------------------------------------------------------------------------------

INSERT INTO snv__consequence_filter_partitioned_int_part
SELECT * FROM snv__consequence_filter_partitioned;


-- ---------------------------------------------------------------------------------------------------
-- 3. Confirm the copy before the swap, while rolling back is still free.
-- ---------------------------------------------------------------------------------------------------
--
--   SELECT part, count(*) FROM snv__consequence_filter_partitioned_int_part GROUP BY part ORDER BY part;
--       -- must match step 0 exactly, part for part
--
--   DESC snv__consequence_filter_partitioned_int_part;
--       -- `part` must read int, everything else identical to the old table
--


-- ---------------------------------------------------------------------------------------------------
-- 4. Atomic name exchange. After this the production name carries the INT column and
--    `..._int_part` carries the old tinyint table.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE snv__consequence_filter_partitioned
    SWAP WITH snv__consequence_filter_partitioned_int_part;


-- ---------------------------------------------------------------------------------------------------
-- 5. Post-checks, read-only. Run these BEFORE step 6 — dropping is the point of no return.
-- ---------------------------------------------------------------------------------------------------
--
--   DESC snv__consequence_filter_partitioned;
--       -- `part` int NO ...
--
--   SHOW CREATE TABLE snv__consequence_filter_partitioned;
--       -- colocate_with must still name <NAMESPACE>.query_group, buckets still 10,
--       -- DUPLICATE KEY still (part, locus_id, is_deleterious)
--
--   SELECT part, count(*) FROM snv__consequence_filter_partitioned GROUP BY part ORDER BY part;
--       -- must match step 0
--
--   SHOW PROC '/colocation_group';
--       -- the group must be stable / balanced, not in an unhealthy state
--


-- ---------------------------------------------------------------------------------------------------
-- 6. Drop the old table, now sitting under the temporary name. Keep it until step 5 is green.
-- ---------------------------------------------------------------------------------------------------

DROP TABLE snv__consequence_filter_partitioned_int_part;


-- ---------------------------------------------------------------------------------------------------
-- 7. Then re-run the part that failed. `insert_snv_consequence_filter_part` is
--    INSERT OVERWRITE with dynamic_overwrite, so replaying import_part for the affected WXS part is
--    idempotent and touches no other part.
-- ---------------------------------------------------------------------------------------------------
--
--   SELECT part, count(*) FROM snv__consequence_filter_partitioned GROUP BY part ORDER BY part;
--       -- expect the WXS part (>= 65536) to appear with a non-zero count
