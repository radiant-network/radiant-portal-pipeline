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
    c.exon.rank,
    c.exon.total,
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
    COALESCE(c.source = 'RefSeq' AND NULLIF(c.mane_pair_transcript_id, '') IS NOT NULL, FALSE)
        AS scores_from_mane_pair,
    c.vep_impact,
    c.aa_change,
    c.dna_change
FROM {{ mapping.iceberg_snv_consequence }} c
LEFT JOIN {{ mapping.starrocks_snv_tmp_variant }} v ON c.locus_hash = v.locus_hash
LEFT JOIN {{ mapping.starrocks_dbnsfp }} d
    ON v.locus_id=d.locus_id
   AND d.ensembl_transcript_id = CASE
           WHEN c.source = 'RefSeq' THEN NULLIF(c.mane_pair_transcript_id, '')
           ELSE NULLIF(c.transcript_id, '')
       END
LEFT JOIN {{ mapping.starrocks_spliceai }} sp ON v.locus_id=sp.locus_id AND sp.symbol = c.symbol
LEFT JOIN {{ mapping.starrocks_gnomad_constraint }} gc
    ON gc.transcript_id = CASE
           WHEN c.source = 'RefSeq' THEN NULLIF(c.mane_pair_transcript_id, '')
           ELSE NULLIF(c.transcript_id, '')
       END
WHERE c.task_id in %(task_ids)s
