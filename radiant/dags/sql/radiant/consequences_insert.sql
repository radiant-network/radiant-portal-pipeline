INSERT INTO {{ params.starrocks_consequences }}
SELECT
    v.locus_id AS locus_id,
    COALESCE(c.symbol, '') AS symbol,
    COALESCE(c.transcript_id, '') AS transcript_id,
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
    c.vep_impact,
    c.aa_change,
    c.dna_change
FROM {{ params.iceberg_consequences }} c
LEFT JOIN {{ params.starrocks_tmp_variants }} v ON c.locus_hash = v.locus_hash
LEFT JOIN {{ params.starrocks_dbnsfp }} d ON v.locus_id=d.locus_id AND d.ensembl_transcript_id = c.transcript_id
LEFT JOIN {{ params.starrocks_spliceai }} sp ON v.locus_id=sp.locus_id AND sp.symbol = c.symbol
LEFT JOIN {{ params.starrocks_gnomad_constraints }} gc ON gc.transcript_id=c.transcript_id
WHERE c.case_id in %(case_ids)s
