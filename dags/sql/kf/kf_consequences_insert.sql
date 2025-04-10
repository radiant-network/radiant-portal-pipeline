INSERT INTO kf_consequences
SELECT
    v.locus_id AS locus_id,
    COALESCE(c.symbol, '') AS symbol,
    COALESCE(c.ensembl_transcript_id, '') AS ensembl_transcript_id,
    c.consequences,
    c.impact_score,
    c.biotype,
    sp.spliceai_ds,
    sp.spliceai_type,
    c.canonical,
    c.picked,
    c.mane_select,
    c.mane_plus,
    s.sift_score,
    s.sift_pred,
    s.polyphen2_hvar_score,
    s.polyphen2_hvar_pred,
    s.fathmm_score,
    s.fathmm_pred,
    s.cadd_score,
    s.cadd_phred,
    s.dann_score,
    s.revel_score,
    s.lrt_score,
    s.lrt_pred,
    s.phyloP17way_primate,
    s.phyloP100way_vertebrate,
    c.aa_change,
    c.coding_dna_change
FROM iceberg.poc_starrocks.kf_consequences c
LEFT JOIN stg_kf_variants v ON c.hash = v.hash
LEFT JOIN dbnsfp s ON v.locus_id=s.locus_id AND s.ensembl_transcript_id = c.ensembl_transcript_id
LEFT JOIN spliceai sp ON v.locus_id=sp.locus_id AND sp.symbol = c.symbol
LEFT ANTI JOIN kf_consequences kf ON kf.locus_id = v.locus_id

