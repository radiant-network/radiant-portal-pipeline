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
    e.gene_combined_score AS exomiser_gene_combined_score,
    ch.comphet_genes IS NOT NULL AS has_comphet,
    ch.comphet_genes
FROM {{ mapping.iceberg_germline_snv_occurrence }} o
JOIN [BROADCAST] (
    SELECT locus_hash, locus_id
    FROM {{ mapping.starrocks_snv_tmp_variant }}
) v ON o.locus_hash = v.locus_hash
LEFT JOIN (
     SELECT e.locus_id, e.seq_id, e.moi, e.acmg_classification, e.acmg_evidence, e.variant_score, e.gene_combined_score
     FROM {{ mapping.starrocks_exomiser }} e
     WHERE e.variant_rank = 1 and e.part={{ partition }}
) e
    ON o.seq_id = e.seq_id
   AND v.locus_id = e.locus_id
LEFT JOIN (
     -- One entry per gene with its number of partners, counted on the rows the portal lists (RAD-39).
     SELECT g.seq_id, g.task_id, g.locus_id,
            array_agg(named_struct('symbol', g.symbol, 'nb', g.nb) ORDER BY g.symbol) AS comphet_genes
     FROM (
          SELECT c.seq_id, c.task_id, c.locus_id, c.symbol, CAST(COUNT(1) AS INT) AS nb
          FROM {{ mapping.starrocks_germline_snv_comphet }} c
          WHERE c.part = {{ partition }} AND c.task_id IN %(task_ids)s
          GROUP BY c.seq_id, c.task_id, c.locus_id, c.symbol
     ) g
     GROUP BY g.seq_id, g.task_id, g.locus_id
) ch
    ON o.seq_id = ch.seq_id
   AND o.task_id = ch.task_id
   AND v.locus_id = ch.locus_id
WHERE o.part = {{ partition }} and o.task_id in %(task_ids)s
AND o.tenant_code = %(tenant_code)s
AND has_alt;
