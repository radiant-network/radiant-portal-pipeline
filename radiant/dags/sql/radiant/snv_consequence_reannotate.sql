-- Re-annotation counterpart of `snv_consequence_insert.sql` (SJRA-1811 §5, Decision 4 Option A).
--
-- Differences from the ingest statement:
--   1. The driving table is `snv__consequence` itself, not `iceberg_snv_consequence`. That is the whole
--      point of the re-annotation: nothing downstream re-reads Iceberg.
--   2. No `task_ids` predicate and no `snv__tmp_variant` join. Both exist in the ingest statement only to
--      cut the scan down to the batch being imported; a re-annotation covers every row.
--   3. `locus_id` is already resolved and stored, so the `locus_hash` -> `snv__tmp_variant` lookup goes.
--
-- `score_transcript_id` is recomputed rather than taken from a stored column, because none is stored.
-- It reproduces the ingest rule exactly (`snv_consequence_insert.sql`): a RefSeq row reads its scores
-- under the Ensembl twin its MANE pair names. §5's illustrative snippet joins dbNSFP on `transcript_id`
-- directly; that would silently drop every RefSeq row's scores and flip `scores_from_mane_pair`, so the
-- ingest rule wins.
--
-- The join hints are carried over unchanged. They are load-bearing, not tuning -- see the comments in
-- `snv_consequence_insert.sql`. The re-annotation scans are strictly larger than the ingest ones (no
-- batch predicate), so the plans they guard against are more likely here, not less.
--
-- `INSERT INTO` on a PRIMARY KEY table is an upsert: the 16 carried-through columns are rewritten as-is
-- and the 18 open-data columns pick up the refreshed reference tables. Self-referencing: StarRocks fixes
-- the read snapshot at plan time, so the scan is not affected by the rows this statement writes.
INSERT INTO {{ mapping.starrocks_snv_consequence }}
SELECT
    c.locus_id,
    c.symbol,
    c.transcript_id,
    c.transcript_version,
    c.source,
    c.consequences,
    c.impact_score,
    c.biotype,
    c.exon_rank,
    c.exon_total,
    sp.spliceai_ds,
    sp.spliceai_type,
    c.is_canonical,
    c.is_picked,
    c.is_mane_select,
    c.is_mane_plus,
    c.mane_select,
    c.mane_pair_transcript_id,
    d.sift_score,
    d.sift_pred,
    d.polyphen2_hvar_score,
    d.polyphen2_hvar_pred,
    d.fathmm_score,
    d.fathmm_pred,
    d.cadd_score,
    d.cadd_phred,
    d.dann_score,
    d.revel_score,
    d.lrt_score,
    d.lrt_pred,
    gc.pli,
    gc.loeuf,
    d.phyloP17way_primate,
    d.phyloP100way_vertebrate,
    COALESCE(c.source = 'RefSeq' AND c.score_transcript_id IS NOT NULL, FALSE)
        AS scores_from_mane_pair,
    c.vep_impact,
    c.aa_change,
    c.dna_change
FROM (
    SELECT
        con.*,
        CASE
            WHEN con.source = 'RefSeq' THEN NULLIF(con.mane_pair_transcript_id, '')
            ELSE NULLIF(con.transcript_id, '')
        END AS score_transcript_id
    FROM {{ mapping.starrocks_snv_consequence }} con
) c
LEFT JOIN [BROADCAST] {{ mapping.starrocks_dbnsfp }} d
    ON d.locus_id = c.locus_id
   AND d.ensembl_transcript_id = c.score_transcript_id
LEFT JOIN [BROADCAST] {{ mapping.starrocks_spliceai }} sp
    ON sp.locus_id = c.locus_id AND sp.symbol = c.symbol
LEFT JOIN [BROADCAST] {{ mapping.starrocks_gnomad_constraint }} gc
    ON gc.transcript_id = c.score_transcript_id
