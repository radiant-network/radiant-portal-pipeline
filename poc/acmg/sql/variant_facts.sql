-- Variant facts: one row per classified (locus, transcript) with every value a variant criterion reads.
-- No rule logic here. ${classified}: the MANE Select consequences in scope of the loci of the cases asked for.
WITH classified AS (
    ${classified}
),
clinvar AS (
    SELECT
        locus_id,
        name,
        chromosome,
        start,
        coalesce(gs.current, split_part(split_part(geneinfo, '|', 1), ':', 1)) AS symbol,
        arrays_overlap(clin_sig, ['Pathogenic']) AS pathogenic,
        arrays_overlap(clin_sig, ['Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic']) AS plp,
        arrays_overlap(clin_sig, ['Benign', 'Likely_benign', 'Benign/Likely_benign']) AS blb,
        array_join(mc, ',') LIKE '%missense_variant%' AS missense,
        array_join(mc, ',') REGEXP 'nonsense|frameshift_variant|splice_donor_variant|splice_acceptor_variant' AS lof
    FROM ${shared_db}.clinvar
    LEFT JOIN ${output_db}.gene_symbol gs ON gs.symbol = split_part(split_part(geneinfo, '|', 1), ':', 1)
    -- Review status: the best star rating among the variant's RCV (one per condition).
    WHERE locus_id IN (
        SELECT locus_id FROM ${shared_db}.clinvar_rcv_summary
        GROUP BY locus_id
        HAVING max(review_status_stars) >= ${clinvar_min_stars}
    )
),
clinvar_locus AS (
    SELECT cv.locus_id, max(IF(cv.plp, 1, 0)) = 1 AS plp, max(IF(cv.plp, r.stars, 0)) AS plp_stars
    FROM clinvar cv
    LEFT JOIN (
        SELECT locus_id, max(review_status_stars) AS stars FROM ${shared_db}.clinvar_rcv_summary GROUP BY locus_id
    ) r ON r.locus_id = cv.locus_id
    GROUP BY cv.locus_id
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
-- PS1/PM5: ClinVar P/LP missense of another variant at the same residue of the classified MANE Select transcript.
missense_residue AS (
    SELECT
        k.locus_id,
        k.transcript_id,
        CAST(regexp_extract(c.aa_change, '^p[.]([A-Z][a-z]{2})([0-9]+)([A-Z][a-z]{2})$$', 2) AS INT) AS position,
        regexp_extract(c.aa_change, '^p[.]([A-Z][a-z]{2})([0-9]+)([A-Z][a-z]{2})$$', 3) AS alt_aa
    FROM classified k
    JOIN ${shared_db}.snv__consequence c ON c.locus_id = k.locus_id AND c.transcript_id = k.transcript_id
    WHERE array_contains(c.consequences, 'missense_variant')
),
same_residue AS (
    SELECT
        m.locus_id,
        m.transcript_id,
        max(IF(p.alt_aa = m.alt_aa, IF(cv.pathogenic, 2, 1), 0)) AS same_aa,
        max(IF(p.alt_aa <> m.alt_aa, IF(cv.pathogenic, 2, 1), 0)) AS same_residue
    FROM missense_residue m
    JOIN ${output_db}.clinvar_protein p ON p.transcript_id = m.transcript_id AND p.position = m.position
    JOIN clinvar cv ON cv.name = p.variation_id AND cv.plp
    WHERE cv.locus_id <> m.locus_id
    GROUP BY m.locus_id, m.transcript_id
),
-- OMIM inheritance of the classified transcript's gene: snv__variant's comes from the variant's picked gene, which
-- can be an overlapping one.
omim AS (
    SELECT coalesce(gs.current, o.symbol) AS symbol,
        array_remove(array_unique_agg(o.inheritance_code), NULL) AS inheritance_code
    FROM ${shared_db}.omim_gene_panel o
    LEFT JOIN ${output_db}.gene_symbol gs ON gs.symbol = o.symbol
    GROUP BY coalesce(gs.current, o.symbol)
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
),
splice_locus AS (
    SELECT k.locus_id, k.transcript_id, replace(v.chromosome, 'chr', '') AS chromosome, v.start
    FROM classified k
    JOIN ${shared_db}.snv__consequence c ON c.locus_id = k.locus_id AND c.transcript_id = k.transcript_id
    JOIN ${tenant_db}.snv__variant v ON v.locus_id = k.locus_id
    WHERE arrays_overlap(c.consequences, ['splice_donor_variant', 'splice_acceptor_variant'])
),
-- Each exon edge goes into its 1 kb bin and both neighbours, so that "within 3 bp" becomes an equi-join (as PM1).
exon_edge AS (
    SELECT edge.chromosome, edge.pos, floor(edge.pos / 1000) + shift.d AS bin,
        edge.transcript_ids, edge.ensembl_phase, edge.ensembl_end_phase
    FROM (
        SELECT replace(chromosome, 'chr', '') AS chromosome, start AS pos, transcript_ids, ensembl_phase,
            ensembl_end_phase
        FROM ${shared_db}.ensembl_exon_by_gene
        UNION ALL
        SELECT replace(chromosome, 'chr', ''), end, transcript_ids, ensembl_phase, ensembl_end_phase
        FROM ${shared_db}.ensembl_exon_by_gene
    ) edge
    CROSS JOIN (SELECT -1 AS d UNION ALL SELECT 0 UNION ALL SELECT 1) shift
),
-- Exon skipped by a canonical splice variant: the transcript's exon with an edge within 3 bp of it. Its frame shifts
-- when its Ensembl start and end phases differ; an exon partly non-coding (phase -1) counts as in frame.
splice_exon AS (
    SELECT
        s.locus_id,
        s.transcript_id,
        max(IF(x.ensembl_phase <> x.ensembl_end_phase AND x.ensembl_phase <> '-1' AND x.ensembl_end_phase <> '-1',
            1, 0)) = 1 AS frameshift
    FROM splice_locus s
    JOIN exon_edge x ON x.chromosome = s.chromosome AND x.bin = floor(s.start / 1000)
    WHERE abs(s.start - x.pos) <= 3
        AND array_contains(array_map(t -> split_part(t, '.', 1), x.transcript_ids), s.transcript_id)
    GROUP BY s.locus_id, s.transcript_id
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
    se.frameshift AS splice_frameshift,
    g.af AS gnomad_af,
    CAST(g.an AS BIGINT) AS gnomad_an,
    CAST(g.hom AS BIGINT) AS gnomad_hom,
    v.germline_pf_wgs_not_affected,
    CAST(v.germline_pn_wgs_not_affected AS BIGINT) AS germline_pn_wgs_not_affected,
    v.germline_pf_wxs_not_affected,
    CAST(v.germline_pn_wxs_not_affected AS BIGINT) AS germline_pn_wxs_not_affected,
    CAST(coalesce(v.germline_hom_wgs_not_affected, 0) + coalesce(v.germline_hom_wxs_not_affected, 0) AS BIGINT)
        AS local_hom_not_affected,
    om.inheritance_code AS omim_inheritance,
    CAST(cgn.hi_score AS BIGINT) AS clingen_hi_score,
    coalesce(cgn.no_disease, FALSE) AS clingen_no_disease,
    coalesce(cl.plp, FALSE) AS clinvar_plp,
    CAST(coalesce(cl.plp_stars, 0) AS BIGINT) AS clinvar_plp_stars,
    CAST(coalesce(w.plp_missense, 0) AS BIGINT) AS clinvar_plp_missense_window,
    CAST(coalesce(w.blb_missense, 0) AS BIGINT) AS clinvar_blb_missense_window,
    CAST(coalesce(sr.same_aa, 0) AS BIGINT) AS clinvar_same_aa,
    CAST(coalesce(sr.same_residue, 0) AS BIGINT) AS clinvar_same_residue,
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
LEFT JOIN splice_exon se ON se.locus_id = c.locus_id AND se.transcript_id = c.transcript_id
LEFT JOIN same_residue sr ON sr.locus_id = c.locus_id AND sr.transcript_id = c.transcript_id
-- Gene joins on the current HGNC symbol: each source has its own release.
LEFT JOIN ${output_db}.gene_symbol gs ON gs.symbol = c.symbol
LEFT JOIN clinvar_gene cg ON cg.symbol = coalesce(gs.current, c.symbol)
LEFT JOIN ${output_db}.clingen_gene cgn ON cgn.symbol = coalesce(gs.current, c.symbol)
LEFT JOIN omim om ON om.symbol = coalesce(gs.current, c.symbol)
