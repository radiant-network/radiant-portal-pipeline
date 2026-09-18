-- Re-annotation counterpart of `snv_staging_variant_insert.sql` (SJRA-1811 §5, Decision 4 Option A).
--
-- Two differences from the ingest statement, and no others:
--   1. The driving table is `snv__staging_variant` itself, not `snv__tmp_variant`. `snv__tmp_variant` holds
--      only the loci of the batch `import_part` is processing; a re-annotation has to cover every locus the
--      platform has ever imported, and the accumulator is the table that holds them.
--   2. No `task_ids` predicate, for the same reason.
--
-- `INSERT INTO` on a PRIMARY KEY table is an upsert, so the 22 carried-through columns are rewritten with
-- the values they already hold and the 7 open-data columns (gnomad_v3_af, topmed_af, tg_af, clinvar_name,
-- clinvar_interpretation, rsnumber, omim_inheritance_code) pick up the refreshed reference tables.
-- Self-referencing: StarRocks fixes the read snapshot at plan time, so the scan is not affected by the
-- rows this statement writes.
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
    om.inheritance_code AS omim_inheritance_code
FROM {{ mapping.starrocks_snv_staging_variant }} v
LEFT JOIN {{ mapping.starrocks_gnomad_genomes_v3 }} g ON g.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_topmed_bravo }} t ON t.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_1000_genomes }} tg ON tg.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_clinvar }} cl  ON cl.locus_id = v.locus_id
LEFT JOIN {{ mapping.starrocks_dbsnp }} d  ON d.locus_id = v.locus_id
LEFT JOIN (SELECT symbol, array_remove(array_unique_agg(inheritance_code), NULL) AS inheritance_code FROM {{ mapping.starrocks_omim_gene_panel }} GROUP BY symbol) om ON om.symbol = v.symbol
