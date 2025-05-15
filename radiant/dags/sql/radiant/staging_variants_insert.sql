INSERT INTO {{ params.starrocks_staging_variants }}
SELECT
    v.locus_id,
    g.af AS gnomad_v3_af,
    t.af AS topmed_af,
    tg.af AS tg_af,
    v.chromosome,
    v.start,
    v.end,
    cl.name AS clinvar_name,
    v.variant_class,
    cl.interpretations AS clinvar_interpretation,
    v.symbol,
    v.impact_score,
    v.consequences,
    v.vep_impact,
    v.is_mane_select,
    v.is_mane_plus,
    v.is_canonical,
    v.rsnumber,
    v.reference,
    v.alternate,
    v.mane_select,
    v.hgvsg,
    v.hgvsc,
    v.hgvsp,
    v.locus,
    v.dna_change,
    v.aa_change,
    v.transcript_id,
    om.inheritance_code AS omim_inheritance_code
FROM {{ params.starrocks_tmp_variants }} v
LEFT JOIN {{ params.starrocks_gnomad_genomes_v3 }} g ON g.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_topmed_bravo }} t ON t.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_1000_genomes }} tg ON tg.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_clinvar }} cl  ON cl.locus_id = v.locus_id
LEFT JOIN {{ params.starrocks_omim_gene_panel }} om  ON om.symbol = v.symbol
