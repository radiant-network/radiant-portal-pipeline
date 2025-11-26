INSERT OVERWRITE {{ mapping.starrocks_germline_snv_tmp_variant }}
SELECT COALESCE(GET_VARIANT_ID(t.chromosome, t.start, t.reference, t.alternate), v.locus_id) as locus_id,
    t.chromosome,
    t.start,
    t.variant_class,
    t.symbol,
    t.impact_score,
    t.consequences,
    t.vep_impact,
    t.is_mane_select,
    t.is_mane_plus,
    t.is_canonical,
    t.rsnumber,
    t.end,
    t.reference,
    t.alternate,
    t.mane_select,
    t.hgvsg,
    t.hgvsc,
    t.hgvsp,
    t.locus,
    t.locus_hash,
    t.dna_change,
    t.aa_change,
    t.transcript_id
FROM {{ mapping.iceberg_variant }} t
LEFT JOIN {{ mapping.starrocks_variant_lookup }} v ON t.locus_hash = v.locus_hash
where t.task_id in %(task_ids)s and t.alternate <> '*';