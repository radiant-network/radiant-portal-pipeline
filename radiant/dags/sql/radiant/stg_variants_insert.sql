INSERT INTO {{ params.starrocks_staging_variants }}
SELECT v.locus_id,
    t.chromosome,
    t.start,
    t.variant_class,
    split(t.symbol, '-')[1] AS symbol, -- some rows have multiple gene symbol separated by '-', need to investigate
    t.consequences AS consequence,
    t.vep_impact,
    t.mane_select,
    t.mane_plus,
    t.canonical,
    t.picked,
    t.rsnumber,
    t.reference,
    t.alternate,
    t.hgvsg,
    t.locus,
    t.hash,
    t.dna_change,
    t.aa_change
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_variants }} t
JOIN {{ params.starrocks_variants_lookup }} v ON t.hash = v.hash
LEFT ANTI JOIN {{ params.starrocks_staging_variants }} stg ON stg.locus_id = v.locus_id