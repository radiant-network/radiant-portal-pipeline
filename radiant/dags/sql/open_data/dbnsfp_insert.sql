INSERT OVERWRITE {{ params.starrocks_dbnsfp }}
SELECT
    COALESCE(GET_VARIANT_ID(d.chromosome, d.start, d.reference, d.alternate), v.locus_id) as locus_id,
	d.ensembl_transcript_id,
    d.SIFT_score AS sift_score,
    d.SIFT_pred AS sift_pred,
    d.Polyphen2_HVAR_score AS polyphen2_hvar_score,
    d.Polyphen2_HVAR_pred AS polyphen2_hvar_pred,
    d.FATHMM_score AS fathmm_score,
    d.FATHMM_pred AS fathmm_pred,
    d.CADD_raw_rankscore AS cadd_score,
    d.CADD_phred AS cadd_phred,
    d.DANN_score AS dann_score,
    d.REVEL_score AS revel_score,
    d.LRT_score AS lrt_score,
    d.LRT_pred AS lrt_pred,
    d.phyloP17way_primate AS phyloP17way_primate,
    d.phyloP100way_vertebrate AS phyloP100way_vertebrate
FROM {{ params.iceberg_dbnsfp }} d
LEFT JOIN {{ params.starrocks_variant_lookup }} v ON d.locus_hash = v.locus_hash