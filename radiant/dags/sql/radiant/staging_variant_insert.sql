INSERT INTO {{ mapping.starrocks_staging_variant }}
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
    d.rsnumber,
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
FROM {{ mapping.starrocks_tmp_variant }} v
LEFT JOIN {{ mapping.starrocks_gnomad_genomes_v3 }} g ON g.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_topmed_bravo }} t ON t.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_1000_genomes }} tg ON tg.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_clinvar }} cl  ON cl.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_dbsnp }} d  ON d.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_omim_gene_panel }} om  ON om.symbol = v.symbol
