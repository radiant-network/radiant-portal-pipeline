-- Occurrence facts: one row per (seq_id, locus) of the cases asked for, where the case has evidence of its own:
-- a de novo call, an affected parent's call, a comphet partner P/LP in ClinVar. No rule logic here.
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
),
-- The best comphet partner of the occurrence: 4 P in trans, 3 P unphased, 2 LP in trans, 1 LP unphased.
-- "In trans" when the case is a complete trio.
comphet AS (
    SELECT
        ch.seq_id,
        ch.locus_id,
        max(CASE
            WHEN cl.pathogenic AND cp.complete_trio THEN 4
            WHEN cl.pathogenic THEN 3
            WHEN cp.complete_trio THEN 2
            ELSE 1
        END) AS category
    FROM ${tenant_db}.germline__snv__comphet ch
    JOIN sequencing s ON s.seq_id = ch.seq_id
    JOIN case_parents cp ON cp.case_id = s.case_id
    JOIN clinvar_locus cl ON cl.locus_id = ch.partner_locus_id AND cl.plp
    WHERE ch.seq_id IN (${cases})
        ${comphet_part}
        AND ch.locus_id IN (SELECT locus_id FROM classified)
    GROUP BY ch.seq_id, ch.locus_id
)
SELECT
    CAST(o.seq_id AS BIGINT) AS seq_id,
    o.locus_id,
    v.omim_inheritance_code AS omim_inheritance,
    CAST(o.de_novo AS BIGINT) AS denovo_probands,
    CAST(o.carrier_parents AS BIGINT) AS affected_carrier_parents,
    CAST(o.noncarrier_parents AS BIGINT) AS affected_noncarrier_parents,
    CAST(IF(ch.category = 4, 1, 0) AS BIGINT) AS comphet_p_trans,
    CAST(IF(ch.category = 2, 1, 0) AS BIGINT) AS comphet_lp_trans,
    CAST(IF(ch.category = 3, 1, 0) AS BIGINT) AS comphet_p_unphased,
    CAST(IF(ch.category = 1, 1, 0) AS BIGINT) AS comphet_lp_unphased
FROM occurrence o
JOIN ${tenant_db}.snv__variant v ON v.locus_id = o.locus_id
LEFT JOIN comphet ch ON ch.seq_id = o.seq_id AND ch.locus_id = o.locus_id
WHERE o.de_novo + o.carrier_parents + o.noncarrier_parents > 0 OR ch.category IS NOT NULL
