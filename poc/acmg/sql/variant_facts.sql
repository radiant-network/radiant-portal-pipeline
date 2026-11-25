-- Variant facts: one row per classified (locus, transcript) with every value a variant criterion reads.
-- No rule logic here. ${classified}: the MANE Select consequences in scope of the loci of the cases asked for.
WITH classified AS (
    ${classified}
),
clinvar AS (
    SELECT
        locus_id,
        chromosome,
        start,
        split_part(split_part(geneinfo, '|', 1), ':', 1) AS symbol,
        arrays_overlap(clin_sig, ['Pathogenic']) AS pathogenic,
        arrays_overlap(clin_sig, ['Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic']) AS plp,
        arrays_overlap(clin_sig, ['Benign', 'Likely_benign', 'Benign/Likely_benign']) AS blb,
        array_join(mc, ',') LIKE '%missense_variant%' AS missense,
        array_join(mc, ',') REGEXP 'nonsense|frameshift_variant|splice_donor_variant|splice_acceptor_variant' AS lof
    FROM ${shared_db}.clinvar
    -- Review status: the best star rating among the variant's RCV (one per condition).
    WHERE locus_id IN (
        SELECT locus_id FROM ${shared_db}.clinvar_rcv_summary
        GROUP BY locus_id
        HAVING max(review_status_stars) >= ${clinvar_min_stars}
    )
),
clinvar_locus AS (
    SELECT locus_id, max(IF(plp, 1, 0)) = 1 AS plp
    FROM clinvar
    GROUP BY locus_id
),
-- PM1 window: each ClinVar variant goes into its position bin and both neighbours, so that the
-- "within window_bp" range join becomes an equi-join on (chromosome, bin).
clinvar_binned AS (
    SELECT cv.*, floor(cv.start / ${pm1_window_bp}) + shift.d AS bin
    FROM clinvar cv
    CROSS JOIN (SELECT -1 AS d UNION ALL SELECT 0 UNION ALL SELECT 1) shift
),
clinvar_window AS (
    SELECT
        v.locus_id,
        sum(IF(cv.plp AND cv.missense, 1, 0)) AS plp_missense,
        sum(IF(cv.blb AND cv.missense, 1, 0)) AS blb_missense
    FROM ${tenant_db}.snv__variant v
    JOIN clinvar_binned cv ON cv.chromosome = v.chromosome AND cv.bin = floor(v.start / ${pm1_window_bp})
    WHERE abs(cv.start - v.start) <= ${pm1_window_bp}
        AND cv.locus_id <> v.locus_id
        AND v.locus_id IN (SELECT locus_id FROM classified)
    GROUP BY v.locus_id
),
clinvar_gene AS (
    SELECT
        symbol,
        sum(IF(plp, 1, 0)) AS plp,
        sum(IF(plp AND missense, 1, 0)) AS plp_missense,
        sum(IF(plp AND lof, 1, 0)) AS plp_lof,
        sum(IF(blb AND missense, 1, 0)) AS blb_missense
    FROM clinvar
    GROUP BY symbol
)
SELECT
    c.locus_id,
    c.transcript_id,
    c.symbol,
    c.consequences,
    CAST(c.revel_score AS DOUBLE) AS revel_score,
    CAST(c.spliceai_ds AS DOUBLE) AS spliceai_ds,
    CAST(c.gnomad_loeuf AS DOUBLE) AS gnomad_loeuf,
    CAST(nullif(c.exon_rank, '') AS BIGINT) AS exon_rank,
    CAST(nullif(c.exon_total, '') AS BIGINT) AS exon_total,
    g.af AS gnomad_af,
    CAST(g.an AS BIGINT) AS gnomad_an,
    CAST(g.hom AS BIGINT) AS gnomad_hom,
    v.germline_pf_wgs_not_affected,
    CAST(v.germline_pn_wgs_not_affected AS BIGINT) AS germline_pn_wgs_not_affected,
    v.germline_pf_wxs_not_affected,
    CAST(v.germline_pn_wxs_not_affected AS BIGINT) AS germline_pn_wxs_not_affected,
    CAST(coalesce(v.germline_hom_wgs_not_affected, 0) + coalesce(v.germline_hom_wxs_not_affected, 0) AS BIGINT)
        AS local_hom_not_affected,
    v.omim_inheritance_code AS omim_inheritance,
    coalesce(cl.plp, FALSE) AS clinvar_plp,
    CAST(coalesce(w.plp_missense, 0) AS BIGINT) AS clinvar_plp_missense_window,
    CAST(coalesce(w.blb_missense, 0) AS BIGINT) AS clinvar_blb_missense_window,
    CAST(coalesce(cg.plp, 0) AS BIGINT) AS gene_plp,
    CAST(coalesce(cg.plp_missense, 0) AS BIGINT) AS gene_plp_missense,
    CAST(coalesce(cg.plp_lof, 0) AS BIGINT) AS gene_plp_lof,
    CAST(coalesce(cg.blb_missense, 0) AS BIGINT) AS gene_blb_missense,
    -- PS4: carriers and sequenced patients of the tenant, affected vs not affected (ETL frequencies).
    CAST(coalesce(v.germline_pc_wgs_affected, 0) + coalesce(v.germline_pc_wxs_affected, 0) AS BIGINT)
        AS affected_carriers,
    CAST(coalesce(v.germline_pn_wgs_affected, 0) + coalesce(v.germline_pn_wxs_affected, 0) AS BIGINT)
        AS affected_total,
    CAST(coalesce(v.germline_pc_wgs_not_affected, 0) + coalesce(v.germline_pc_wxs_not_affected, 0) AS BIGINT)
        AS not_affected_carriers,
    CAST(coalesce(v.germline_pn_wgs_not_affected, 0) + coalesce(v.germline_pn_wxs_not_affected, 0) AS BIGINT)
        AS not_affected_total
FROM ${shared_db}.snv__consequence c
JOIN classified k ON k.locus_id = c.locus_id AND k.transcript_id = c.transcript_id
JOIN ${tenant_db}.snv__variant v ON v.locus_id = c.locus_id
LEFT JOIN ${shared_db}.gnomad_genomes_v3 g ON g.locus_id = c.locus_id
LEFT JOIN clinvar_locus cl ON cl.locus_id = c.locus_id
LEFT JOIN clinvar_window w ON w.locus_id = c.locus_id
LEFT JOIN clinvar_gene cg ON cg.symbol = c.symbol
