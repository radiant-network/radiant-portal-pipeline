INSERT OVERWRITE {{ params.starrocks_staging_variants }}
SELECT v.locus_id,
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
JOIN {{ params.starrocks_variants_lookup }} v ON t.locus_hash = v.locus_hash
where t.case_id in %(case_ids)s;