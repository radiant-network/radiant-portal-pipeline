-- Re-annotation counterpart of `snv_staging_variant_insert.sql` (SJRA-1811 §5, Decision 4 Option A).
--
-- Two differences from the ingest statement, and no others:
--   1. The driving table is `snv__staging_variant` itself, not `snv__tmp_variant`. `snv__tmp_variant` holds
--      only the loci of the batch `import_part` is processing; a re-annotation has to cover every locus the
--      platform has ever imported, and the accumulator is the table that holds them.
--   2. No `task_ids` predicate, for the same reason.
--
INSERT INTO {{ mapping.starrocks_snv_staging_variant }}
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
    v.pick_source,
    om.inheritance_code AS omim_inheritance_code,
    -- COSMIC CMC (RAD-15): the locus row when there is one, otherwise the HGVS row of the picked transcript.
    -- The whole row falls back at once (CASE on cms.locus_id, not a COALESCE per column), so the four
    -- fields of a variant never mix two COSMIC rows.
    CASE WHEN cms.locus_id IS NOT NULL THEN cms.mutation_url   ELSE cmh.mutation_url   END AS cmc_mutation_url,
    CASE WHEN cms.locus_id IS NOT NULL THEN cms.sample_mutated ELSE cmh.sample_mutated END AS cmc_sample_mutated,
    CASE WHEN cms.locus_id IS NOT NULL THEN cms.sample_ratio   ELSE cmh.sample_ratio   END AS cmc_sample_ratio,
    CASE WHEN cms.locus_id IS NOT NULL THEN cms.tier           ELSE cmh.tier           END AS cmc_tier
FROM {{ mapping.starrocks_snv_staging_variant }} v
LEFT JOIN {{ mapping.starrocks_gnomad_genomes_v3 }} g ON g.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_topmed_bravo }} t ON t.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_1000_genomes }} tg ON tg.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_clinvar }} cl  ON cl.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_dbsnp }} d  ON d.locus_id = v.locus_id
LEFT JOIN (SELECT symbol, array_remove(array_unique_agg(inheritance_code), NULL) AS inheritance_code FROM {{ mapping.starrocks_omim_gene_panel }} GROUP BY symbol) om ON om.symbol = v.symbol
LEFT JOIN {{ mapping.starrocks_cosmic_mutation_set }} cms ON cms.locus_id = v.locus_id
-- Small table (only the mutations coordinates cannot place), keyed on the version-free transcript and the
-- `c.` change: the same shape as the picked consequence's transcript_id and dna_change.
LEFT JOIN [BROADCAST] {{ mapping.starrocks_cosmic_mutation_set_hgvs }} cmh
       ON cmh.transcript_id = v.transcript_id AND cmh.cds_change = v.dna_change
