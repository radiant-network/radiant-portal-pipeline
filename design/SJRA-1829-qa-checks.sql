-- =====================================================================================
-- SJRA-1829 — post-load verification for the merged VEP (RefSeq) ingestion.
--
-- Checks §10 of design/SJRA-1820-vep-merged-refseq-ingestion.md against a real load.
-- Written for the WGS chr21 / 2-case run:
--     base database   : radiant                       (snv__consequence, snv__consequence_filter)
--     tenant database : onekg_dragen_4_4_7_tenant     (snv__variant, germline__snv__occurrence)
--
-- Run top to bottom. Every check names its own pass condition; the ones that produce numbers
-- rather than a verdict are marked RECORD.
--
-- SCOPE. `radiant.snv__consequence` is shared and holds ~73M pre-existing rows from earlier,
-- non-merged loads. Nearly every check therefore restricts to the loci this run touched. Two
-- scope definitions are used, and they are not interchangeable:
--
--   occ_loci  = DISTINCT locus_id FROM the tenant occurrence table — every locus ingested.
--   var_loci  = locus_id FROM the tenant snv__variant — the subset that also survived the
--               tenant_loci semi-join in snv_variant_insert.sql. This is where pick_source lives.
--
-- Check 2b compares the two on purpose: during a mid-run probe they disagreed (85,635 vs
-- 124,971), which is either a still-running frequency step or a real loss. Re-run it once the
-- DAG is green before reading anything into it.
--
-- One caveat that applies throughout: consequence extraction applies no FILTER/PASS predicate
-- (radiant/tasks/vcf/snv/consequence.py writes every CSQ block of every record), so VCF-side
-- counts must not be filtered either.
-- =====================================================================================


-- =====================================================================================
-- 1. Classification is exhaustive
--    Every consequence row is Ensembl or RefSeq, except transcript-less intergenic rows,
--    which are counted and reported rather than silently dropped.
--
--    PASS: for source IS NULL, no_transcript = rows_ (every unclassified row is genuinely
--          transcript-less). Any NULL-source row WITH a transcript is a classification miss
--          and means resolve_source() fell through all four rules.
--    RECORD: the NULL-source row count — this is the "counted and reported" number.
-- =====================================================================================

WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT c.source,
       count(*)                                                AS rows_,
       count(DISTINCT c.locus_id)                              AS loci,
       sum(CASE WHEN c.transcript_id = '' THEN 1 ELSE 0 END)   AS no_transcript,
       sum(CASE WHEN c.symbol = '' THEN 1 ELSE 0 END)          AS no_symbol,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)      AS pct
FROM radiant.snv__consequence c
JOIN m ON m.locus_id = c.locus_id
GROUP BY 1
ORDER BY 2 DESC;

-- The strict form of the same assertion, as one number.
-- PASS: 0.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT count(*) AS unclassified_but_has_transcript
FROM radiant.snv__consequence c
JOIN m ON m.locus_id = c.locus_id
WHERE c.source IS NULL
  AND c.transcript_id <> '';

-- Closed value set: nothing outside Ensembl / RefSeq / NULL ever reaches the column.
-- PASS: 0.
SELECT count(*) AS unexpected_source_values
FROM radiant.snv__consequence
WHERE source IS NOT NULL
  AND source NOT IN ('Ensembl', 'RefSeq');


-- =====================================================================================
-- 2. Nothing is lost
-- =====================================================================================

-- 2a. StarRocks row count vs the VCF annotation-block count.
--
-- The VCF side, run against the merged file (no FILTER predicate — see the caveat above):
--
--   # raw annotation blocks
--   bcftools query -f '%INFO/CSQ\n' merged.vcf.gz \
--     | awk '{n=split($0,a,","); print n}' | paste -sd+ - | bc
--
--   # blocks deduplicated on the StarRocks primary key (locus, SYMBOL, versionless Feature).
--   # Adjust $SYM/$FEAT to the 1-based CSQ field positions in your file:
--   #   bcftools +split-vep -l merged.vcf.gz
--   bcftools query -f '%CHROM\t%POS\t%REF\t%ALT\t%INFO/CSQ\n' merged.vcf.gz \
--     | awk -F'\t' -v SYM=$SYM -v FEAT=$FEAT '{
--         n = split($5, blocks, ",")
--         for (i = 1; i <= n; i++) {
--           split(blocks[i], f, "|")
--           feat = f[FEAT]; sub(/\..*$/, "", feat)
--           print $1":"$2":"$3":"$4"|"f[SYM]"|"feat
--         }}' | sort -u | wc -l
--
-- PASS: the deduplicated VCF count equals `rows_` summed over check 1. The raw count will be
--       higher — that gap is the primary-key collapse and is expected; a gap between the
--       *deduplicated* count and StarRocks is a loss.
--
-- 2b. The two locus scopes must agree, or the difference must be explained.
-- RECORD both; investigate if occ_loci > var_loci once the DAG is green.
SELECT (SELECT count(DISTINCT locus_id) FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence) AS occ_loci,
       (SELECT count(*)                 FROM onekg_dragen_4_4_7_tenant.snv__variant)              AS var_loci,
       (SELECT count(DISTINCT locus_id) FROM radiant.snv__consequence c
         WHERE EXISTS (SELECT 1 FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence o
                        WHERE o.locus_id = c.locus_id))                                           AS csq_loci;

-- 2c. Every ingested locus has at least one consequence row.
-- PASS: 0.
SELECT count(*) AS loci_with_no_consequence
FROM (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence) o
LEFT ANTI JOIN (SELECT DISTINCT locus_id FROM radiant.snv__consequence) c ON c.locus_id = o.locus_id;

-- 2d. The Ensembl half must match, block for block, what a non-merged run produces.
-- Requires a privileged connection — the MCP user has no SELECT on the Iceberg catalog.
-- Run this one as the pipeline's StarRocks user:
--
--   SELECT source, count(*) AS iceberg_rows,
--          count(DISTINCT concat_ws('|', cast(locus_hash AS varchar),
--                                        coalesce(symbol, ''), coalesce(transcript_id, ''))) AS distinct_pk
--   FROM radiant_iceberg_catalog.radiant.snv_consequence
--   WHERE task_id IN (<the task_ids of this run>)
--   GROUP BY 1;
--
-- PASS: distinct_pk per source equals the StarRocks per-source count from check 1.
--       iceberg_rows > distinct_pk is fine (duplicate blocks collapse on the key);
--       distinct_pk > StarRocks rows is a silent primary-key collision — see check 4.


-- =====================================================================================
-- 3. (§10.3) No regression on old files — the pre-existing rows are untouched and labelled.
--    PASS: a single Ensembl / false row plus the intergenic NULL bucket; the Ensembl count
--          equals the pre-migration total (73,040,174 at the time the baseline was taken)
--          plus whatever this run added.
-- =====================================================================================

SELECT source, scores_from_mane_pair, count(*) AS rows_,
       count(sift_score) AS with_sift, count(gnomad_pli) AS with_pli
FROM radiant.snv__consequence
GROUP BY 1, 2
ORDER BY 1, 2;


-- =====================================================================================
-- 4. The primary key still separates the two catalogues, and is version-stable
-- =====================================================================================

-- 4a. Namespace disjointness — the assumption carrying the weight a source partition key
--     would otherwise have carried (§6). No transcript_id may exist under both sources.
-- PASS: 0.
SELECT count(*) AS transcript_ids_claimed_by_both_sources
FROM (
  SELECT transcript_id FROM radiant.snv__consequence WHERE source = 'Ensembl' AND transcript_id <> '' GROUP BY 1
  INTERSECT
  SELECT transcript_id FROM radiant.snv__consequence WHERE source = 'RefSeq'  AND transcript_id <> '' GROUP BY 1
) x;

-- 4b. Each source stays inside its own namespace.
-- PASS: all zeros.
SELECT
  sum(CASE WHEN source = 'RefSeq'  AND transcript_id NOT REGEXP '^[NX][MRP]_' THEN 1 ELSE 0 END)                              AS refseq_outside_namespace,
  sum(CASE WHEN source = 'Ensembl' AND transcript_id NOT REGEXP '^(ENST|ENSR|LRG_)' AND transcript_id <> '' THEN 1 ELSE 0 END) AS ensembl_outside_namespace
FROM radiant.snv__consequence;

-- 4c. Version stability. A version reaching the key would make the next RefSeq release
--     duplicate rows instead of replacing them, silently.
-- PASS: all zeros.
SELECT
 (SELECT count(*) FROM radiant.snv__consequence WHERE transcript_id LIKE '%.%')                             AS tid_versioned,
 (SELECT count(*) FROM radiant.snv__consequence WHERE mane_pair_transcript_id LIKE '%.%')                    AS pair_versioned,
 (SELECT count(*) FROM radiant.snv__consequence WHERE source = 'Ensembl' AND transcript_version IS NOT NULL) AS ensembl_with_version,
 (SELECT count(*) FROM radiant.snv__consequence WHERE source = 'RefSeq'  AND transcript_version IS NULL)     AS refseq_without_version;

-- 4d. The read path must not change. Pick any high-consequence locus and profile the
--     variant-page point lookup: it should still touch a single tablet, ~3.4 ms.
--
--   SET enable_profile = true;
--   SELECT locus_id, symbol, transcript_id, source, transcript_version, consequences,
--          impact_score, vep_impact, is_mane_select, scores_from_mane_pair, aa_change, dna_change
--   FROM radiant.snv__consequence
--   WHERE locus_id = <pick one from the query below>;
--   SHOW PROFILELIST;
--   ANALYZE PROFILE FROM '<query_id>';
--
--   -- read from the profile: OLAP_SCAN_NODE TabletCount = 1, plus the total query time.
SELECT locus_id, count(*) AS csq_rows
FROM radiant.snv__consequence c
WHERE EXISTS (SELECT 1 FROM onekg_dragen_4_4_7_tenant.snv__variant v WHERE v.locus_id = c.locus_id
                AND v.pick_source = 'RefSeq')
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;


-- 4e. `symbol` is in the primary key and is NOT version-stable.
--
--     §10.4 guards `transcript_id` against a version suffix reaching the key, because a
--     RefSeq release bump would then duplicate rows instead of replacing them, silently.
--     `symbol` sits in the same key and has exactly the same instability, and nothing
--     guards it: when a gene is renamed between two annotation-cache versions, the new load
--     writes (locus, NEW_SYMBOL, transcript) beside the existing (locus, OLD_SYMBOL,
--     transcript) rather than over it. The same transcript then exists twice at one locus,
--     under two gene names, and only the newer row carries the MANE flags and the pairing.
--
--     Found on the chr21 run: 7,387 (locus, transcript) pairs / 14,774 rows across 68
--     transcripts — C21orf62 -> EPCIP, C21orf62-AS1 -> EPCIP-AS1, CLDN14-AS1 <-> LNCTSI,
--     D21S2088E -> '', '' -> CYYR1-AS1, '' -> LINC00310, ANKRD20A11P -> ''.
--
--     This is NOT caused by the merged/RefSeq work — both duplicate rows are source=Ensembl.
--     It is a cache-version upgrade colliding with the primary key, and any future cache bump
--     reproduces it genome-wide. It surfaced here only because this is the first load with a
--     newer cache.
--
--     RECORD the numbers. Non-zero on a run whose loci were never loaded before is impossible
--     and would mean the merged file itself emits two symbols for one transcript.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT count(*)                        AS locus_transcript_pairs_with_2plus_symbols,
       sum(n)                          AS rows_involved,
       count(DISTINCT transcript_id)   AS distinct_transcripts
FROM (
  SELECT c.locus_id, c.transcript_id, count(*) AS n
  FROM radiant.snv__consequence c
  JOIN m ON m.locus_id = c.locus_id
  WHERE c.source = 'Ensembl' AND c.transcript_id <> ''
  GROUP BY 1, 2
  HAVING count(DISTINCT c.symbol) > 1
) x;

-- The renamed genes themselves, for the ticket.
SELECT transcript_id,
       group_concat(DISTINCT symbol)                 AS symbols,
       count(*)                                      AS rows_,
       sum(cast(is_mane_select AS int))              AS mane_rows,
       sum(CASE WHEN nullif(mane_pair_transcript_id, '') IS NULL THEN 1 ELSE 0 END) AS rows_without_a_pair
FROM radiant.snv__consequence
WHERE source = 'Ensembl' AND transcript_id IN (
  SELECT transcript_id FROM radiant.snv__consequence
  WHERE source = 'Ensembl' AND transcript_id <> ''
  GROUP BY 1 HAVING count(DISTINCT symbol) > 1)
GROUP BY 1
ORDER BY 3 DESC;

-- Blast radius: are the duplicates confined to the loci this run wrote, or table-wide?
-- Confined to this run's loci = this load created them. Spread across the table = earlier
-- loads did it too and it has been accumulating unnoticed.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence),
d AS (SELECT c.locus_id, c.transcript_id
      FROM radiant.snv__consequence c
      WHERE c.source = 'Ensembl' AND c.transcript_id <> ''
      GROUP BY 1, 2 HAVING count(DISTINCT c.symbol) > 1)
SELECT sum(CASE WHEN m.locus_id IS NOT NULL THEN 1 ELSE 0 END) AS on_this_runs_loci,
       sum(CASE WHEN m.locus_id IS NULL     THEN 1 ELSE 0 END) AS elsewhere_in_the_table,
       count(*)                                                AS total_pairs
FROM d LEFT JOIN m ON m.locus_id = d.locus_id;

-- Confirmation that the stale row is a leftover rather than something the merged file emitted.
-- Requires the privileged connection (see 2d). If a transcript appears under exactly ONE
-- symbol in Iceberg for this run's task_ids, the second symbol in StarRocks is a leftover:
--
--   SELECT transcript_id, count(DISTINCT symbol) AS symbols_in_this_run
--   FROM radiant_iceberg_catalog.radiant.snv_consequence
--   WHERE task_id IN (<the task_ids of this run>) AND source = 'Ensembl' AND transcript_id <> ''
--   GROUP BY 1 HAVING count(DISTINCT symbol) > 1;
--   -- expect 0 rows


-- =====================================================================================
-- 5. The headline block is coherent
--    pick_source names the catalogue, and every headline field comes from that same
--    transcript. No variant mixes fields from two transcripts.
-- =====================================================================================

-- 5a. Distribution. RECORD: the reference file put RefSeq at 1.4% of variants. A WGS chr21
--     run will read higher (intergenic-heavy), so compare pick_refseq / (pick_refseq +
--     pick_ensembl) rather than pick_refseq / variants, and record both.
SELECT pick_source, count(*) AS variants,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_of_all
FROM onekg_dragen_4_4_7_tenant.snv__variant
GROUP BY 1
ORDER BY 2 DESC;

-- 5b. Internal coherence of the headline block, as one row of zeros.
--     hgvsc embeds a VERSIONED accession while transcript_id is version-free, hence the
--     prefix match rather than equality (§5).
-- PASS: all zeros.
SELECT
  count(*)                                                                                                        AS variants,
  sum(CASE WHEN pick_source = 'RefSeq'  AND transcript_id NOT REGEXP '^[NX][MRP]_' THEN 1 ELSE 0 END)              AS refseq_pick_wrong_transcript,
  sum(CASE WHEN pick_source = 'Ensembl' AND transcript_id NOT REGEXP '^(ENST|ENSR|LRG_)'
                                        AND transcript_id <> '' THEN 1 ELSE 0 END)                                 AS ensembl_pick_wrong_transcript,
  sum(CASE WHEN pick_source IS NULL AND transcript_id <> '' THEN 1 ELSE 0 END)                                     AS null_pick_with_transcript,
  sum(CASE WHEN pick_source = 'RefSeq'  AND hgvsc <> '' AND hgvsc NOT LIKE concat(transcript_id, '.%') THEN 1 ELSE 0 END) AS refseq_hgvsc_other_transcript,
  sum(CASE WHEN pick_source = 'Ensembl' AND hgvsc <> '' AND hgvsc NOT LIKE concat(transcript_id, '%')  THEN 1 ELSE 0 END) AS ensembl_hgvsc_other_transcript,
  sum(CASE WHEN pick_source = 'RefSeq'  AND hgvsp <> '' AND hgvsp NOT REGEXP '^[NX]P_' THEN 1 ELSE 0 END)          AS refseq_hgvsp_wrong_namespace,
  sum(CASE WHEN pick_source = 'Ensembl' AND hgvsp <> '' AND hgvsp NOT LIKE 'ENSP%' THEN 1 ELSE 0 END)              AS ensembl_hgvsp_wrong_namespace,
  sum(CASE WHEN dna_change <> '' AND hgvsc <> '' AND hgvsc NOT LIKE concat('%:', dna_change) THEN 1 ELSE 0 END)     AS dna_change_not_from_hgvsc,
  sum(CASE WHEN aa_change  <> '' AND hgvsp <> '' AND hgvsp NOT LIKE concat('%:', aa_change)  THEN 1 ELSE 0 END)     AS aa_change_not_from_hgvsp
FROM onekg_dragen_4_4_7_tenant.snv__variant;

-- 5c. The headline transcript must exist as a real consequence row, and carry the same
--     symbol / consequences / dna_change / aa_change. This is the check that catches a
--     headline assembled from two different transcripts.
-- PASS: 0 for both columns.
SELECT
  sum(CASE WHEN c.locus_id IS NULL THEN 1 ELSE 0 END) AS headline_transcript_missing_from_csq,
  sum(CASE WHEN c.locus_id IS NOT NULL
            AND (coalesce(c.dna_change, '') <> coalesce(v.dna_change, '')
              OR coalesce(c.aa_change,  '') <> coalesce(v.aa_change,  '')
              OR coalesce(c.vep_impact, '') <> coalesce(v.vep_impact, '')
              OR c.impact_score <> v.impact_score) THEN 1 ELSE 0 END) AS headline_fields_disagree_with_csq
FROM onekg_dragen_4_4_7_tenant.snv__variant v
LEFT JOIN radiant.snv__consequence c
       ON c.locus_id = v.locus_id
      AND c.symbol = coalesce(v.symbol, '')
      AND c.transcript_id = coalesce(v.transcript_id, '')
WHERE v.transcript_id <> '';

-- 5d. pick_source must equal the source of that same consequence row.
-- PASS: 0.
SELECT count(*) AS pick_source_disagrees_with_csq_source
FROM onekg_dragen_4_4_7_tenant.snv__variant v
JOIN radiant.snv__consequence c
     ON c.locus_id = v.locus_id
    AND c.symbol = coalesce(v.symbol, '')
    AND c.transcript_id = coalesce(v.transcript_id, '')
WHERE v.transcript_id <> ''
  AND coalesce(c.source, '') <> coalesce(v.pick_source, '');


-- =====================================================================================
-- 6. MANE pairs are consistent
--    Both catalogues flag MANE Select on the same gene, and each side's cross-reference
--    points at the other's transcript.
--
--    NOTE: MANE Plus Clinical rows are excluded from the reciprocity assertion on purpose.
--    VEP fills MANE_SELECT only on the Select transcript, so is_mane_plus rows have no
--    pointer to reciprocate (see the comment in consequence.py).
-- =====================================================================================

-- 6a. Both sides are populated at all. PASS: neither side is 0; they should be close.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT c.source,
       sum(CASE WHEN c.is_mane_select THEN 1 ELSE 0 END)                     AS mane_select_rows,
       sum(CASE WHEN c.is_mane_plus   THEN 1 ELSE 0 END)                     AS mane_plus_rows,
       count(nullif(c.mane_pair_transcript_id, ''))                          AS with_cross_reference,
       count(DISTINCT CASE WHEN c.is_mane_select THEN c.locus_id END)        AS mane_loci
FROM radiant.snv__consequence c
JOIN m ON m.locus_id = c.locus_id
GROUP BY 1
ORDER BY 1;

-- 6b. Reciprocity: the RefSeq MANE row's pointer resolves to an Ensembl row at the same
--     locus, that row is also flagged MANE Select, and its pointer points back.
--
--     The twin is matched on (locus_id, transcript_id) and NOT on symbol. Two reasons, both
--     found on the chr21 run:
--       - the two catalogues can disagree on the gene name for the same transcript (Ensembl
--         emits an empty SYMBOL where RefSeq says `LOC128092249`), and
--       - the same Ensembl transcript can be stored twice at one locus under two symbols,
--         because `symbol` is in the primary key and is not version-stable (see check 4e).
--     Matching on symbol turns both into phantom failures. The twin candidates are collapsed
--     with an aggregate first, so the second case cannot fan the row count out either.
--
-- PASS: pairs_ok = refseq_mane_rows; pointer_resolves_to_nothing and twin_not_flagged_mane
--       are 0. twin_under_a_different_symbol is RECORD, not a failure — but a non-zero
--       reading is the symptom check 4e quantifies, so follow it there.
SELECT
  count(*)                                                                          AS refseq_mane_rows,
  sum(CASE WHEN t.locus_id IS NULL THEN 1 ELSE 0 END)                                AS pointer_resolves_to_nothing,
  sum(CASE WHEN t.locus_id IS NOT NULL AND t.twin_is_mane = 0 THEN 1 ELSE 0 END)     AS twin_not_flagged_mane,
  sum(CASE WHEN t.locus_id IS NOT NULL
            AND coalesce(t.twin_pair, '') <> r.transcript_id THEN 1 ELSE 0 END)      AS pointer_not_reciprocal,
  sum(CASE WHEN t.locus_id IS NOT NULL AND t.twin_symbols > 1 THEN 1 ELSE 0 END)     AS twin_stored_under_2plus_symbols,
  sum(CASE WHEN t.locus_id IS NOT NULL AND t.twin_symbols = 1
            AND t.a_symbol <> r.symbol THEN 1 ELSE 0 END)                            AS twin_under_a_different_symbol,
  sum(CASE WHEN t.locus_id IS NOT NULL AND t.twin_is_mane = 1
            AND t.twin_pair = r.transcript_id THEN 1 ELSE 0 END)                     AS pairs_ok
FROM radiant.snv__consequence r
LEFT JOIN (
  SELECT locus_id,
         transcript_id,
         max(cast(is_mane_select AS int))         AS twin_is_mane,
         max(mane_pair_transcript_id)             AS twin_pair,
         count(DISTINCT symbol)                   AS twin_symbols,
         min(symbol)                              AS a_symbol
  FROM radiant.snv__consequence
  WHERE source = 'Ensembl' AND transcript_id <> ''
  GROUP BY 1, 2
) t ON t.locus_id = r.locus_id
   AND t.transcript_id = r.mane_pair_transcript_id
WHERE r.source = 'RefSeq'
  AND r.is_mane_select
  AND nullif(r.mane_pair_transcript_id, '') IS NOT NULL;

-- 6c. Coverage. RECORD: the reference file paired 94.7% of variants. A large drop means
--     --mane was lost upstream.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT count(*) AS ingested_loci,
       sum(CASE WHEN p.locus_id IS NOT NULL THEN 1 ELSE 0 END) AS loci_with_a_mane_pair,
       round(100.0 * sum(CASE WHEN p.locus_id IS NOT NULL THEN 1 ELSE 0 END) / count(*), 2) AS pct
FROM m
LEFT JOIN (SELECT DISTINCT locus_id FROM radiant.snv__consequence
            WHERE source = 'RefSeq' AND is_mane_select
              AND nullif(mane_pair_transcript_id, '') IS NOT NULL) p
       ON p.locus_id = m.locus_id;


-- =====================================================================================
-- 7. Only the new RefSeq annotations reach the filter table
-- =====================================================================================

-- 7a. Growth. RECORD the total and compare against your pre-merged baseline; the table is
--     INSERT OVERWRITE, so this is a full-rebuild number, not a delta.
--     PASS: growth on the merged loci is ~11%, not ~100%.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence),
ens AS (SELECT DISTINCT c.locus_id, c.symbol, unnest AS consequence
        FROM radiant.snv__consequence c, UNNEST(consequences) AS unnest
        WHERE c.source = 'Ensembl')
SELECT count(*)                                                                   AS filter_rows_on_merged_loci,
       sum(CASE WHEN e.locus_id IS NULL THEN 1 ELSE 0 END)                        AS refseq_only_rows,
       round(100.0 * sum(CASE WHEN e.locus_id IS NULL THEN 1 ELSE 0 END)
                   / nullif(sum(CASE WHEN e.locus_id IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS pct_growth_over_ensembl
FROM radiant.snv__consequence_filter f
JOIN m ON m.locus_id = f.locus_id
LEFT JOIN ens e ON e.locus_id = f.locus_id AND e.symbol = f.symbol AND e.consequence = f.consequence;

-- 7b. No gene/consequence combination appears twice for the same variant because of source
--     doubling.
--
--     A naive `count(*) > 1` on (locus_id, symbol, consequence) does NOT answer this. The
--     insert's own GROUP BY includes biotype, vep_impact, impact_score AND sift_score,
--     polyphen2_hvar_score, fathmm_score, revel_score, spliceai_ds, gnomad_pli — so two
--     transcripts of one gene sharing a consequence but differing on any score legitimately
--     produce two rows. That predates this story: measured on the chr21 run, 43,984 of 331,205
--     groups have >1 row, and the same query on the loci this run did NOT touch reads 16.1%
--     against 11.2% here — the merged loci are below the pre-existing background rate.
--
--     Two assertions actually answer §10.7. Both passed on the chr21 run.

--     7b-i. No two filter rows are genuinely identical. Signature = the insert's full GROUP BY
--           key. Any surplus is a row the insert could not have produced.
-- PASS: groups_with_truly_identical_rows = 0 AND surplus_rows = 0.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT count(*)                                          AS groups_,
       sum(CASE WHEN n > full_sigs THEN 1 ELSE 0 END)    AS groups_with_truly_identical_rows,
       sum(n) - sum(full_sigs)                           AS surplus_rows
FROM (
  SELECT f.locus_id, f.symbol, f.consequence,
         count(*) AS n,
         count(DISTINCT concat_ws('|', coalesce(f.biotype, ''), coalesce(f.vep_impact, ''),
               cast(f.impact_score AS varchar), cast(f.sift_score AS varchar),
               cast(f.polyphen2_hvar_score AS varchar), cast(f.fathmm_score AS varchar),
               cast(f.revel_score AS varchar), cast(f.spliceai_ds AS varchar),
               cast(f.gnomad_pli AS varchar))) AS full_sigs
  FROM radiant.snv__consequence_filter f
  JOIN m ON m.locus_id = f.locus_id
  GROUP BY 1, 2, 3
) g;

--     7b-ii. No duplication is attributable to RefSeq. The anti join in
--            snv_consequence_filter_insert.sql keys on (locus_id, symbol, consequence), so a
--            group is either Ensembl-provided or RefSeq-only and the two can never mix inside
--            one group. Splitting the duplicate count by origin therefore isolates exactly the
--            duplication this story could have introduced.
-- PASS: suspicious_groups on the `refseq-only` row = 0. The `ensembl-provided` row is RECORD —
--       compare it against the same query with `LEFT ANTI JOIN m` for the background rate.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence),
ens AS (SELECT DISTINCT c.locus_id, c.symbol, unnest AS consequence
        FROM radiant.snv__consequence c, UNNEST(consequences) AS unnest
        WHERE c.source = 'Ensembl'),
g AS (
  SELECT f.locus_id, f.symbol, f.consequence,
         count(*) AS n,
         count(DISTINCT concat_ws('|', coalesce(f.biotype, ''), coalesce(f.vep_impact, ''),
                                       cast(f.impact_score AS varchar))) AS sigs,
         max(CASE WHEN e.locus_id IS NOT NULL THEN 1 ELSE 0 END) AS ensembl_provides
  FROM radiant.snv__consequence_filter f
  JOIN m ON m.locus_id = f.locus_id
  LEFT JOIN ens e ON e.locus_id = f.locus_id AND e.symbol = f.symbol AND e.consequence = f.consequence
  GROUP BY 1, 2, 3)
SELECT CASE WHEN ensembl_provides = 1 THEN 'ensembl-provided' ELSE 'refseq-only' END AS origin,
       count(*)                                                      AS groups_,
       sum(CASE WHEN n > 1 THEN 1 ELSE 0 END)                        AS dup_groups,
       sum(CASE WHEN n > sigs THEN 1 ELSE 0 END)                     AS suspicious_groups,
       round(100.0 * sum(CASE WHEN n > sigs THEN 1 ELSE 0 END) / count(*), 2) AS pct_suspicious
FROM g
GROUP BY 1
ORDER BY 2 DESC;

-- 7c. The RefSeq-only HIGH-impact findings are still findable (79 on the reference file, in
--     genes such as TPM2 and SMARCD3). RECORD the gene list for this run.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence),
ens AS (SELECT DISTINCT c.locus_id, c.symbol, unnest AS consequence
        FROM radiant.snv__consequence c, UNNEST(consequences) AS unnest
        WHERE c.source = 'Ensembl')
SELECT f.symbol, f.consequence, count(*) AS rows_, count(DISTINCT f.locus_id) AS loci
FROM radiant.snv__consequence_filter f
JOIN m ON m.locus_id = f.locus_id
LEFT ANTI JOIN ens e ON e.locus_id = f.locus_id AND e.symbol = f.symbol AND e.consequence = f.consequence
WHERE f.vep_impact = 'HIGH'
GROUP BY 1, 2
ORDER BY 3 DESC;

-- 7d. Nothing that Ensembl already provides was dropped: every Ensembl (locus, symbol,
--     consequence) on the merged loci must still be present in the filter table.
-- PASS: 0.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT count(*) AS ensembl_combinations_missing_from_filter
FROM (
  SELECT DISTINCT c.locus_id, c.symbol, unnest AS consequence
  FROM radiant.snv__consequence c
  JOIN m ON m.locus_id = c.locus_id, UNNEST(c.consequences) AS unnest
  WHERE c.source = 'Ensembl'
) e
LEFT ANTI JOIN radiant.snv__consequence_filter f
  ON f.locus_id = e.locus_id AND f.symbol = e.symbol AND f.consequence = e.consequence;


-- =====================================================================================
-- 8. Borrowed scores are correct and labelled
-- =====================================================================================

-- 8a. The flag is only ever set where it should be.
-- PASS: no Ensembl or NULL-source row carries it; every flagged row is RefSeq with a pointer;
--       flagged rows must include non-null sift_score AND gnomad_pli (all-null means a
--       versioned key reached the join and it silently matched nothing).
SELECT source, scores_from_mane_pair,
       count(*)                                        AS rows_,
       count(nullif(mane_pair_transcript_id, ''))      AS with_pointer,
       count(sift_score)                               AS with_sift,
       count(cadd_score)                               AS with_cadd,
       count(gnomad_pli)                               AS with_pli,
       count(spliceai_ds)                              AS with_spliceai
FROM radiant.snv__consequence
WHERE source = 'RefSeq' OR scores_from_mane_pair
GROUP BY 1, 2
ORDER BY 1, 2;

-- 8b. A non-MANE RefSeq row shows no borrowed scores at all.
--     PASS: 0. (spliceai_ds is excluded — it joins on symbol, not transcript, so it is
--     legitimately populated on RefSeq rows regardless of MANE status.)
SELECT count(*) AS non_mane_refseq_rows_with_borrowed_scores
FROM radiant.snv__consequence
WHERE source = 'RefSeq'
  AND nullif(mane_pair_transcript_id, '') IS NULL
  AND (sift_score IS NOT NULL OR polyphen2_hvar_score IS NOT NULL OR fathmm_score IS NOT NULL
    OR cadd_score IS NOT NULL OR dann_score IS NOT NULL OR revel_score IS NOT NULL
    OR lrt_score IS NOT NULL OR phyloP17way_primate IS NOT NULL
    OR phyloP100way_vertebrate IS NOT NULL OR gnomad_pli IS NOT NULL OR gnomad_loeuf IS NOT NULL);

-- 8c. A borrowed row shows exactly its twin's scores — never a different value.
--     The twin is matched on (locus_id, transcript_id) and aggregated first, for the same
--     reason as 6b: `symbol` is in the primary key but is not stable across cache versions
--     (check 4e), so joining on it both misses twins and fans the row count out.
-- PASS: identical_to_twin = flagged_rows_with_a_twin, divergent_from_twin = 0.
--       chr21 run: 37,962 / 37,962 / 0.
SELECT
  count(*) AS flagged_rows_with_a_twin,
  sum(CASE WHEN r.sift_score              <=> e.sift_score
            AND r.polyphen2_hvar_score    <=> e.polyphen2_hvar_score
            AND r.fathmm_score            <=> e.fathmm_score
            AND r.cadd_score              <=> e.cadd_score
            AND r.cadd_phred              <=> e.cadd_phred
            AND r.dann_score              <=> e.dann_score
            AND r.revel_score             <=> e.revel_score
            AND r.lrt_score               <=> e.lrt_score
            AND r.phyloP17way_primate     <=> e.phyloP17way_primate
            AND r.phyloP100way_vertebrate <=> e.phyloP100way_vertebrate
            AND r.gnomad_pli              <=> e.gnomad_pli
            AND r.gnomad_loeuf            <=> e.gnomad_loeuf
           THEN 1 ELSE 0 END) AS identical_to_twin,
  sum(CASE WHEN NOT (r.sift_score              <=> e.sift_score
                 AND r.polyphen2_hvar_score    <=> e.polyphen2_hvar_score
                 AND r.fathmm_score            <=> e.fathmm_score
                 AND r.cadd_score              <=> e.cadd_score
                 AND r.cadd_phred              <=> e.cadd_phred
                 AND r.dann_score              <=> e.dann_score
                 AND r.revel_score             <=> e.revel_score
                 AND r.lrt_score               <=> e.lrt_score
                 AND r.phyloP17way_primate     <=> e.phyloP17way_primate
                 AND r.phyloP100way_vertebrate <=> e.phyloP100way_vertebrate
                 AND r.gnomad_pli              <=> e.gnomad_pli
                 AND r.gnomad_loeuf            <=> e.gnomad_loeuf)
           THEN 1 ELSE 0 END) AS divergent_from_twin
FROM radiant.snv__consequence r
JOIN (
  SELECT locus_id, transcript_id,
         max(sift_score) AS sift_score, max(polyphen2_hvar_score) AS polyphen2_hvar_score,
         max(fathmm_score) AS fathmm_score, max(cadd_score) AS cadd_score,
         max(cadd_phred) AS cadd_phred, max(dann_score) AS dann_score,
         max(revel_score) AS revel_score, max(lrt_score) AS lrt_score,
         max(phyloP17way_primate) AS phyloP17way_primate,
         max(phyloP100way_vertebrate) AS phyloP100way_vertebrate,
         max(gnomad_pli) AS gnomad_pli, max(gnomad_loeuf) AS gnomad_loeuf
  FROM radiant.snv__consequence
  WHERE source = 'Ensembl' AND transcript_id <> ''
  GROUP BY 1, 2
) e ON e.locus_id = r.locus_id AND e.transcript_id = r.mane_pair_transcript_id
WHERE r.scores_from_mane_pair;

-- 8d. A worked example to eyeball — one MANE pair side by side.
SELECT source, transcript_id, transcript_version, mane_pair_transcript_id, is_mane_select,
       scores_from_mane_pair, sift_score, cadd_phred, revel_score, gnomad_pli, spliceai_ds
FROM radiant.snv__consequence
WHERE locus_id = (SELECT locus_id FROM radiant.snv__consequence
                   WHERE scores_from_mane_pair AND sift_score IS NOT NULL LIMIT 1)
ORDER BY source, transcript_id;


-- =====================================================================================
-- 9. Volume is as predicted — RECORD, not pass/fail. This is the input for sizing
--    production growth.
--
--    Baseline, measured before any merged file was ingested:
--        73,040,174 rows | 5.5 GB | 16.76M distinct loci | 4.36 consequences per locus
--    Expectation for snv__consequence: ~2x. A 5x reading is the surprise to catch here.
-- =====================================================================================

SELECT count(*)                                                        AS rows_,
       count(DISTINCT locus_id)                                        AS distinct_loci,
       round(count(*) / count(DISTINCT locus_id), 2)                   AS csq_per_locus,
       sum(CASE WHEN source = 'Ensembl' THEN 1 ELSE 0 END)             AS ensembl_rows,
       sum(CASE WHEN source = 'RefSeq'  THEN 1 ELSE 0 END)             AS refseq_rows,
       sum(CASE WHEN source IS NULL     THEN 1 ELSE 0 END)             AS intergenic_rows
FROM radiant.snv__consequence;

-- Same three ratios restricted to the merged loci — this is the per-file multiplier that
-- actually projects to production, since the table-wide numbers are diluted by 73M
-- pre-existing Ensembl-only rows.
WITH m AS (SELECT DISTINCT locus_id FROM onekg_dragen_4_4_7_tenant.germline__snv__occurrence)
SELECT count(*)                                                                             AS rows_,
       count(DISTINCT c.locus_id)                                                           AS distinct_loci,
       round(count(*) / count(DISTINCT c.locus_id), 2)                                      AS csq_per_locus,
       round(1.0 * count(*) / nullif(sum(CASE WHEN c.source <> 'RefSeq' THEN 1 ELSE 0 END), 0), 3) AS multiplier_vs_ensembl_only
FROM radiant.snv__consequence c
JOIN m ON m.locus_id = c.locus_id;

-- On-disk size. Compare snv__consequence against the 5.5 GB baseline.
SHOW DATA FROM radiant;

-- NOTE: `SHOW DATA FROM radiant` fails ("Table radiant is not found") — from a session already
-- in the radiant database, use the bare `SHOW DATA` above. information_schema.tables is denied
-- to the MCP user (no SELECT on COLUMN TABLE_NAME), so SHOW DATA is the only route there.
--
-- Load duration: read it off the Airflow task durations for the import_part run.
--
-- ---------------------------------------------------------------------------------------------
-- Measured on the chr21 / 2-case WGS run (2026-08-26), against the pre-merged baseline of
-- 73,040,174 rows / 5.5 GB / 16.76M distinct loci / 4.36 consequences per locus:
--
--   snv__consequence          73,901,814 rows (+1.18%) | 6.594 GB (+19.9%) | 16,766,832 loci | 4.41/locus
--     of which  Ensembl       67,763,564
--               RefSeq           424,998
--               intergenic     5,713,252
--   snv__consequence_filter      223.883 MB
--
--   Per-file multiplier on the 124,971 loci this run touched:
--     total 1,477,053 rows / 11.82 per locus
--     non-RefSeq 1,052,885 rows / 8.43 per locus
--     => 1.403x, well under the ~2x expectation and nowhere near a 5x surprise.
--
--   Two caveats before this number is used for production sizing:
--     - Size grew 19.9% while rows grew 1.18%. That is the four new columns being materialised
--       across all 73.9M rows plus the migration's full-table UPDATE on a primary-key table; some
--       of it is uncompacted data versions. Re-read SHOW DATA after compaction settles.
--     - 11.82 consequences per locus here vs 4.36 table-wide is mostly WGS-vs-WXS, not the merge.
--       The merge's own contribution is the 1.403x, measured on the same loci.
-- ---------------------------------------------------------------------------------------------
