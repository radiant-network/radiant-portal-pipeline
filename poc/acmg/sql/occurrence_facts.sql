-- Occurrence facts: one row per (seq_id, locus) of the cases asked for with evidence: of the case itself (a de novo
-- call, an affected parent's call) or of the cohort on the locus. De novo (PS2) and comphet (PM3) counts are over the
-- tenant's affected probands, one per family (ClinGen SVI points add up over unrelated probands). No rule logic here.
WITH classified AS (
    ${classified}
),
clinvar_locus AS (
    SELECT
        locus_id,
        max(IF(arrays_overlap(clin_sig, ['Pathogenic']), 1, 0)) = 1 AS pathogenic,
        max(IF(arrays_overlap(clin_sig, ['Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic']), 1, 0)) = 1
            AS plp
    FROM ${shared_db}.clinvar
    -- Review status: the best star rating among the variant's RCV (one per condition).
    WHERE locus_id IN (
        SELECT locus_id FROM ${shared_db}.clinvar_rcv_summary
        GROUP BY locus_id
        HAVING max(review_status_stars) >= ${clinvar_min_stars}
    )
    GROUP BY locus_id
),
-- OMIM inheritance of the classified transcript's gene, not of the variant's picked gene (see variant_facts.sql).
omim_locus AS (
    SELECT k.locus_id, any_value(om.inheritance_code) AS inheritance_code
    FROM classified k
    JOIN ${shared_db}.snv__consequence c ON c.locus_id = k.locus_id AND c.transcript_id = k.transcript_id
    -- Gene join on the current HGNC symbol (see variant_facts.sql).
    LEFT JOIN ${output_db}.gene_symbol gs ON gs.symbol = c.symbol
    JOIN (
        SELECT coalesce(g.current, o.symbol) AS symbol,
            array_remove(array_unique_agg(o.inheritance_code), NULL) AS inheritance_code
        FROM ${shared_db}.omim_gene_panel o
        LEFT JOIN ${output_db}.gene_symbol g ON g.symbol = o.symbol
        GROUP BY coalesce(g.current, o.symbol)
    ) om ON om.symbol = coalesce(gs.current, c.symbol)
    GROUP BY k.locus_id
),
sequencing AS (
    SELECT DISTINCT case_id, seq_id, family_role, affected_status
    FROM ${shared_db}.staging_sequencing_experiment
    WHERE analysis_type = 'germline'
        AND tenant_code = '${tenant_code}'
        AND NOT deleted
),
case_parents AS (
    SELECT
        case_id,
        max(IF(family_role = 'father', affected_status, NULL)) AS father_status,
        max(IF(family_role = 'mother', affected_status, NULL)) AS mother_status,
        max(IF(family_role = 'father', 1, 0)) + max(IF(family_role = 'mother', 1, 0)) = 2 AS complete_trio
    FROM sequencing
    GROUP BY case_id
),
-- Parents that somalier finds parent-child with the proband: high relatedness, almost no site sharing no allele
-- (IBS0), unlike a sibling.
confirmed_parents AS (
    SELECT p.seq_id, count(DISTINCT r.seq_id) AS parents
    FROM sequencing p
    JOIN sequencing r ON r.case_id = p.case_id AND r.family_role IN ('mother', 'father')
    JOIN ${output_db}.sample_relatedness s
        ON (s.seq_id_a = p.seq_id AND s.seq_id_b = r.seq_id) OR (s.seq_id_b = p.seq_id AND s.seq_id_a = r.seq_id)
    WHERE s.relatedness >= ${parent_min_relatedness} AND s.ibs0 <= ${parent_max_ibs0_share} * s.n
    GROUP BY p.seq_id
),
-- Every affected proband of the tenant. Its de novo counts for the cohort, and its comphet is "in trans", only when
-- both parents are confirmed: the trios with a wrong parent call thousands of false de novo.
cohort AS (
    SELECT DISTINCT s.seq_id, coalesce(cpa.parents, 0) = 2 AS parentage_confirmed,
        coalesce(cpa.parents, 0) = 2 AND cp.complete_trio AS trans
    FROM sequencing s
    JOIN case_parents cp ON cp.case_id = s.case_id
    LEFT JOIN confirmed_parents cpa ON cpa.seq_id = s.seq_id
    WHERE s.family_role = 'proband' AND s.affected_status = 'affected'
),
-- Other probands' evidence counts on rare variants only, with PM2's general rarity (not the recessive-gene one):
-- gnomAD and the tenant's not affected (AF from the carrier frequency, Hardy-Weinberg). A common variant recurs in many
-- probands: in trans with the same P/LP partner, or as the same false de novo. The case's own evidence always counts (a recessive founder is not that rare).
rare_locus AS (
    SELECT k.locus_id
    FROM (SELECT DISTINCT locus_id FROM classified) k
    JOIN ${tenant_db}.snv__variant v ON v.locus_id = k.locus_id
    LEFT JOIN ${shared_db}.gnomad_genomes_v3 g ON g.locus_id = k.locus_id
    WHERE coalesce(g.af, 0) <= ${pm2_max_af}
        AND coalesce(1 - sqrt(1 - greatest(
            IF(v.germline_pn_wgs_not_affected >= ${cohort_min_pn}, v.germline_pf_wgs_not_affected, 0),
            IF(v.germline_pn_wxs_not_affected >= ${cohort_min_pn}, v.germline_pf_wxs_not_affected, 0)
        )), 0) <= ${pm2_cohort_max_pf}
),
cohort_de_novo AS (
    SELECT DISTINCT o.seq_id, o.locus_id
    FROM ${tenant_db}.germline__snv__occurrence o
    JOIN cohort c ON c.seq_id = o.seq_id AND c.parentage_confirmed
    WHERE o.transmission_mode_source = 'slivar'
        AND o.transmission_mode IN ('autosomal_dominant_de_novo', 'x_linked_dominant_de_novo',
                                    'x_linked_recessive_de_novo')
        AND o.locus_id IN (SELECT locus_id FROM classified)
),
-- The best comphet partner of each proband: 4 P in trans, 3 P unphased, 2 LP in trans, 1 LP unphased.
cohort_comphet AS (
    SELECT
        ch.seq_id,
        ch.locus_id,
        max(CASE WHEN cl.pathogenic AND c.trans THEN 4 WHEN cl.pathogenic THEN 3 WHEN c.trans THEN 2 ELSE 1 END)
            AS category,
        max_by(ch.partner_locus_id,
            CASE WHEN cl.pathogenic AND c.trans THEN 4 WHEN cl.pathogenic THEN 3 WHEN c.trans THEN 2 ELSE 1 END)
            AS partner_locus_id
    FROM ${tenant_db}.germline__snv__comphet ch
    JOIN cohort c ON c.seq_id = ch.seq_id
    JOIN clinvar_locus cl ON cl.locus_id = ch.partner_locus_id AND cl.plp
    WHERE ch.locus_id IN (SELECT locus_id FROM classified)
    GROUP BY ch.seq_id, ch.locus_id
),
cohort_evidence AS (
    SELECT seq_id, locus_id FROM cohort_de_novo WHERE locus_id IN (SELECT locus_id FROM rare_locus)
    UNION
    SELECT seq_id, locus_id FROM cohort_comphet WHERE locus_id IN (SELECT locus_id FROM rare_locus)
),
related AS (
    SELECT seq_id_a AS a, seq_id_b AS b FROM ${output_db}.sample_relatedness
    WHERE relatedness >= ${parent_min_relatedness}
    UNION
    SELECT seq_id_b, seq_id_a FROM ${output_db}.sample_relatedness
    WHERE relatedness >= ${parent_min_relatedness}
),
-- A proband first-degree related to (or a duplicate of) another proband with evidence on the same locus: the smaller
-- seq_id counts for the family.
shadowed AS (
    SELECT DISTINCT e.seq_id, e.locus_id
    FROM cohort_evidence e
    JOIN related r ON r.a = e.seq_id
    JOIN cohort_evidence f ON f.seq_id = r.b AND f.locus_id = e.locus_id
    WHERE f.seq_id < e.seq_id
),
cohort_locus AS (
    SELECT
        e.locus_id,
        sum(IF(d.seq_id IS NULL, 0, 1)) AS denovo_confirmed,
        sum(IF(ch.category = 4, 1, 0)) AS p_trans,
        sum(IF(ch.category = 2, 1, 0)) AS lp_trans,
        sum(IF(ch.category = 3, 1, 0)) AS p_unphased,
        sum(IF(ch.category = 1, 1, 0)) AS lp_unphased
    FROM cohort_evidence e
    LEFT JOIN shadowed sh ON sh.seq_id = e.seq_id AND sh.locus_id = e.locus_id
    LEFT JOIN cohort_de_novo d ON d.seq_id = e.seq_id AND d.locus_id = e.locus_id
    LEFT JOIN cohort_comphet ch ON ch.seq_id = e.seq_id AND ch.locus_id = e.locus_id
    WHERE sh.seq_id IS NULL
    GROUP BY e.locus_id
),
-- Parent calls: max allele 0 = called reference, >= 1 = carrier; a no-call (-1) or no parent counts nowhere.
occurrence AS (
    SELECT
        o.seq_id,
        o.locus_id,
        max(IF(o.transmission_mode_source = 'slivar'
            AND o.transmission_mode IN ('autosomal_dominant_de_novo', 'x_linked_dominant_de_novo',
                                        'x_linked_recessive_de_novo'), 1, 0)) AS de_novo,
        max(IF(cp.father_status = 'affected' AND array_max(o.father_calls) >= 1, 1, 0))
            + max(IF(cp.mother_status = 'affected' AND array_max(o.mother_calls) >= 1, 1, 0)) AS carrier_parents,
        max(IF(cp.father_status = 'affected' AND array_max(o.father_calls) = 0, 1, 0))
            + max(IF(cp.mother_status = 'affected' AND array_max(o.mother_calls) = 0, 1, 0)) AS noncarrier_parents
    FROM ${tenant_db}.germline__snv__occurrence o
    JOIN sequencing s ON s.seq_id = o.seq_id
    JOIN case_parents cp ON cp.case_id = s.case_id
    WHERE o.seq_id IN (${cases})
        ${occurrence_part}
        AND o.locus_id IN (SELECT locus_id FROM classified)
    GROUP BY o.seq_id, o.locus_id
)
SELECT
    CAST(o.seq_id AS BIGINT) AS seq_id,
    o.locus_id,
    om.inheritance_code AS omim_inheritance,
    -- Cohort counts on a rare variant (the case included), else the case's own.
    CAST(coalesce(cl.denovo_confirmed, IF(coalesce(cpa.parents, 0) = 2, o.de_novo, 0)) AS BIGINT) AS denovo_confirmed,
    -- PM6: the case's own unconfirmed de novo only.
    CAST(IF(coalesce(cpa.parents, 0) = 2, 0, o.de_novo) AS BIGINT) AS denovo_assumed,
    CAST(o.carrier_parents AS BIGINT) AS affected_carrier_parents,
    CAST(o.noncarrier_parents AS BIGINT) AS affected_noncarrier_parents,
    CAST(coalesce(cl.p_trans, IF(ch.category = 4, 1, 0)) AS BIGINT) AS comphet_p_trans,
    CAST(coalesce(cl.lp_trans, IF(ch.category = 2, 1, 0)) AS BIGINT) AS comphet_lp_trans,
    CAST(coalesce(cl.p_unphased, IF(ch.category = 3, 1, 0)) AS BIGINT) AS comphet_p_unphased,
    CAST(coalesce(cl.lp_unphased, IF(ch.category = 1, 1, 0)) AS BIGINT) AS comphet_lp_unphased,
    ch.partner_locus_id AS comphet_partner_locus_id
FROM occurrence o
LEFT JOIN omim_locus om ON om.locus_id = o.locus_id
LEFT JOIN cohort_comphet ch ON ch.seq_id = o.seq_id AND ch.locus_id = o.locus_id
LEFT JOIN cohort_locus cl ON cl.locus_id = o.locus_id
LEFT JOIN confirmed_parents cpa ON cpa.seq_id = o.seq_id
WHERE o.de_novo + o.carrier_parents + o.noncarrier_parents > 0 OR ch.category IS NOT NULL OR cl.locus_id IS NOT NULL
