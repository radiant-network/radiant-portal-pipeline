INSERT INTO {{ mapping.starrocks_snv_consequence }}
SELECT
    v.locus_id AS locus_id,
    COALESCE(c.symbol, '') AS symbol,
    COALESCE(c.transcript_id, '') AS transcript_id,
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
        locus_hash,
        symbol,
        transcript_id,
        transcript_version,
        source,
        consequences,
        impact_score,
        biotype,
        exon.rank AS exon_rank,
        exon.total AS exon_total,
        is_canonical,
        is_picked,
        is_mane_select,
        is_mane_plus,
        mane_select,
        mane_pair_transcript_id,
        vep_impact,
        aa_change,
        dna_change,
        -- A RefSeq row reads its scores under the Ensembl twin its MANE pair names.
        CASE
            WHEN source = 'RefSeq' THEN NULLIF(mane_pair_transcript_id, '')
            ELSE NULLIF(transcript_id, '')
        END AS score_transcript_id
    FROM {{ mapping.iceberg_snv_consequence }}
    -- No literal per-cent sign in this file: task_ids binds via pymysql printf paramstyle.
    WHERE task_id in %(task_ids)s
) c
-- INNER, not LEFT: locus_id leads the target primary key and is NOT NULL, so unmatched rows cannot land.
-- [SHUFFLE] is required, not tuning: without it the planner may broadcast the Iceberg stream as the
-- hash build side (51GB+ per node observed) and probe with the small table.
JOIN [SHUFFLE] {{ mapping.starrocks_snv_tmp_variant }} v ON c.locus_hash = v.locus_hash
-- Only this batch's loci can match: the colocate semi joins cut dbnsfp 239M -> ~101k, spliceai 80M -> ~14k.
-- [BROADCAST] is required, not tuning: without it the ~600M-row stream becomes a RIGHT OUTER build side, ~120GB.
LEFT JOIN [BROADCAST] (
    SELECT d.*
    FROM {{ mapping.starrocks_dbnsfp }} d
    LEFT SEMI JOIN {{ mapping.starrocks_snv_tmp_variant }} tv ON d.locus_id = tv.locus_id
) d
    ON v.locus_id = d.locus_id
   AND d.ensembl_transcript_id = c.score_transcript_id
LEFT JOIN [BROADCAST] (
    SELECT s.*
    FROM {{ mapping.starrocks_spliceai }} s
    LEFT SEMI JOIN {{ mapping.starrocks_snv_tmp_variant }} tv ON s.locus_id = tv.locus_id
) sp ON v.locus_id = sp.locus_id AND sp.symbol = c.symbol
-- [BROADCAST] is required, not tuning: without it the planner may invert this outer join and build
-- on the full joined stream (28GB observed) instead of this ~20k-row table.
LEFT JOIN [BROADCAST] {{ mapping.starrocks_gnomad_constraint }} gc
    ON gc.transcript_id = c.score_transcript_id
