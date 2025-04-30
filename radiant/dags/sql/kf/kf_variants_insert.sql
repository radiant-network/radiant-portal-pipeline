INSERT INTO {{ params.starrocks_variants }}
SELECT
    v.locus_id,
    vf.ac / 22000 AS af,
    vf.pc / 11000 AS pf,
    g.af AS gnomad_af,
    t.af AS topmed_af,
    tg.af AS tg_af,
    vf.ac AS ac,
    vf.pc AS pc,
    vf.hom AS hom,
    v.chromosome,
    v.start,
    v.variant_class,
    cl.interpretations AS clinvar_interpretation,
    v.symbol,
    v.consequence,
    v.vep_impact,
    v.mane_select,
    v.mane_plus,
    v.picked,
    v.canonical,
    v.rsnumber,
    v.reference,
    v.alternate,
    v.hgvsg,
    v.locus,
    v.dna_change,
    v.aa_change
FROM {{ params.starrocks_staging_variants }} v
LEFT JOIN {{ params.starrocks_variants_frequences }} vf ON vf.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_gnomad_genomes_v3 }} g ON g.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_topmed_bravo }} t ON t.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_1000_genomes }} tg ON tg.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_clinvar }} cl ON cl.locus_id = v.locus_id
LEFT ANTI JOIN {{ params.starrocks_variants }} kf ON kf.locus_id = v.locus_id