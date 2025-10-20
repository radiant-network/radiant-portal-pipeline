-- Load one PART of SOMATIC SNV occurrences from Iceberg -> StarRocks
-- Requires these mapping keys:
--   {{ mapping.iceberg_somatic_snv_occurrence }}     -- source Iceberg table
--   {{ mapping.starrocks_somatic_occurrence }}       -- target StarRocks table
--   {{ mapping.starrocks_tmp_variant }}              -- variant lookup (has locus_id by locus_hash)

INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_somatic_occurrence }}
SELECT
    o.part,
    o.seq_id,
    o.task_id,
    v.locus_id,                           -- join to resolve the stable locus_id
    /* common/info */
    o.quality,
    o.filter,
    o.info_baseq_rank_sum,
    o.info_excess_het,
    o.info_fs,
    o.info_ds,
    o.info_fraction_informative_reads,
    o.info_inbreed_coeff,
    o.info_mleac,
    o.info_mleaf,
    o.info_mq,
    o.info_mq0,
    o.info_m_qrank_sum,
    o.info_qd,
    o.info_r2_5p_bias,
    o.info_read_pos_rank_sum,
    o.info_sor,
    o.info_vqslod,
    o.info_culprit,
    o.info_dp,
    o.info_haplotype_score,
    o.info_old_record,
    o.info_hotspotallele,
    o.info_cal,

    /* tumor FORMAT */
    o.tumor_calls,
    o.tumor_dp,
    o.tumor_gq,
    o.tumor_has_alt,
    o.tumor_af,
    o.tumor_zygosity,
    o.tumor_ad_ref,
    o.tumor_ad_alt,
    o.tumor_ad_total,
    o.tumor_ad_ratio,
    o.tumor_phased,
    o.tumor_gt_status,

    /* normal FORMAT */
    o.normal_calls,
    o.normal_dp,
    o.normal_gq,
    o.normal_has_alt,
    o.normal_af,
    o.normal_zygosity,
    o.normal_ad_ref,
    o.normal_ad_alt,
    o.normal_ad_total,
    o.normal_ad_ratio,
    o.normal_phased,
    o.normal_gt_status,

    /* meta */
    o.analysis_type,
    o.aliquot
FROM {{ mapping.iceberg_somatic_snv_occurrence }} o
JOIN {{ mapping.starrocks_tmp_variant }} v
  ON o.locus_hash = v.locus_hash
WHERE o.part = %(part)s
  AND COALESCE(o.tumor_has_alt, FALSE);
