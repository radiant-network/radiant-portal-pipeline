INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_occurrence }}
SELECT
    o.part,
    o.seq_id,
    o.task_id,
    v.locus_id,
    ad_ratio,
    gq,
    dp,
    ad_total,
    ad_ref,
    ad_alt,
    zygosity,
    calls,
    quality,
    filter,
    info_baseq_rank_sum,
    info_excess_het,
    info_fs,
    info_ds,
    info_fraction_informative_reads,
    info_inbreed_coeff,
    info_mleac,
    info_mleaf,
    info_mq,
    info_m_qrank_sum,
    info_qd,
    info_r2_5p_bias,
    info_read_pos_rank_sum,
    info_sor,
	info_vqslod,
    info_culprit,
    info_dp,
    info_haplotype_score,
    phased,
    parental_origin,
    father_dp,
    father_gq,
    father_ad_ref,
    father_ad_alt,
    father_ad_total,
    father_ad_ratio,
    father_calls,
    father_zygosity,
    mother_dp,
    mother_gq,
    mother_ad_ref,
    mother_ad_alt,
    mother_ad_total,
    mother_ad_ratio,
    mother_calls,
    mother_zygosity,
    transmission_mode,
    info_old_record,
    e.moi AS exomiser_moi,
    e.acmg_classification AS exomiser_acmg_classification,
    e.acmg_evidence AS exomiser_acmg_evidence,
    e.variant_score AS exomiser_variant_score,
    e.gene_combined_score AS exomiser_gene_combined_score
FROM {{ mapping.iceberg_occurrence }} o
JOIN {{ mapping.starrocks_tmp_variant }} v ON o.locus_hash = v.locus_hash
LEFT JOIN (
     SELECT
        *
     FROM {{ mapping.starrocks_exomiser }} e
     WHERE e.variant_rank = 1
) e
    ON o.seq_id = e.seq_id
   AND v.locus_id = e.locus_id
WHERE o.part = %(part)s
AND has_alt;