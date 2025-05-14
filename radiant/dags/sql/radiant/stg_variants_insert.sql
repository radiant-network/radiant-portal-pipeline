INSERT OVERWRITE {{ params.starrocks_staging_variants }}
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
FROM {{ params.iceberg_variants }} t
LEFT JOIN {{ params.starrocks_variants_lookup }} v ON t.locus_hash = v.locus_hash
where t.case_id in %(case_ids)s and t.alternate <> '*';