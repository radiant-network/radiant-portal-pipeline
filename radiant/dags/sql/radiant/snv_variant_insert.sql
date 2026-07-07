INSERT OVERWRITE {{ mapping.starrocks_snv_variant }}
WITH germline_freq AS (
    SELECT locus_id,
           SUM(pc_wgs)              AS pc_wgs,
           SUM(pc_wgs_affected)     AS pc_wgs_affected,
           SUM(pc_wgs_not_affected) AS pc_wgs_not_affected,
           SUM(pc_wxs)              AS pc_wxs,
           SUM(pc_wxs_affected)     AS pc_wxs_affected,
           SUM(pc_wxs_not_affected) AS pc_wxs_not_affected
    FROM (
        {% for t in tenants %}
        SELECT locus_id, pc_wgs, pc_wgs_affected, pc_wgs_not_affected, pc_wxs, pc_wxs_affected, pc_wxs_not_affected
        FROM {{ per_tenant_mapping(t).starrocks_germline_snv_variant_frequency }}
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ) g
    GROUP BY locus_id
),
germline_pn AS (
    SELECT SUM(pn_wgs)              AS pn_wgs,
           SUM(pn_wgs_affected)     AS pn_wgs_affected,
           SUM(pn_wgs_not_affected) AS pn_wgs_not_affected,
           SUM(pn_wxs)              AS pn_wxs,
           SUM(pn_wxs_affected)     AS pn_wxs_affected,
           SUM(pn_wxs_not_affected) AS pn_wxs_not_affected
    FROM (
        {% for t in tenants %}
        SELECT ANY_VALUE(pn_wgs) AS pn_wgs, ANY_VALUE(pn_wgs_affected) AS pn_wgs_affected,
               ANY_VALUE(pn_wgs_not_affected) AS pn_wgs_not_affected, ANY_VALUE(pn_wxs) AS pn_wxs,
               ANY_VALUE(pn_wxs_affected) AS pn_wxs_affected, ANY_VALUE(pn_wxs_not_affected) AS pn_wxs_not_affected
        FROM {{ per_tenant_mapping(t).starrocks_germline_snv_variant_frequency }}
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ) g
),
somatic_freq AS (
    SELECT locus_id, SUM(pc_tn_wgs) AS pc_tn_wgs, SUM(pc_tn_wxs) AS pc_tn_wxs
    FROM (
        {% for t in tenants %}
        SELECT locus_id, pc_tn_wgs, pc_tn_wxs
        FROM {{ per_tenant_mapping(t).starrocks_somatic_snv_variant_frequency }}
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ) s
    GROUP BY locus_id
),
somatic_pn AS (
    SELECT SUM(pn_tn_wgs) AS pn_tn_wgs, SUM(pn_tn_wxs) AS pn_tn_wxs
    FROM (
        {% for t in tenants %}
        SELECT ANY_VALUE(pn_tn_wgs) AS pn_tn_wgs, ANY_VALUE(pn_tn_wxs) AS pn_tn_wxs
        FROM {{ per_tenant_mapping(t).starrocks_somatic_snv_variant_frequency }}
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ) s
)
SELECT
    v.locus_id,
    COALESCE(COALESCE(gf.pc_wgs, 0) / NULLIF((SELECT pn_wgs FROM germline_pn), 0), 0)       AS germline_pf_wgs,
    COALESCE(COALESCE(gf.pc_wxs, 0) / NULLIF((SELECT pn_wxs FROM germline_pn), 0), 0)       AS germline_pf_wxs,
    COALESCE(COALESCE(sf.pc_tn_wgs, 0) / NULLIF((SELECT pn_tn_wgs FROM somatic_pn), 0), 0)  AS somatic_pf_tn_wgs,
    COALESCE(COALESCE(sf.pc_tn_wxs, 0) / NULLIF((SELECT pn_tn_wxs FROM somatic_pn), 0), 0)  AS somatic_pf_tn_wxs,
    v.gnomad_v3_af,
    v.topmed_af,
    v.tg_af,
    COALESCE(gf.pc_wgs, 0)                                                                  AS germline_pc_wgs,
    COALESCE((SELECT pn_wgs FROM germline_pn), 0)                                           AS germline_pn_wgs,
    COALESCE(gf.pc_wgs_affected, 0)                                                         AS germline_pc_wgs_affected,
    COALESCE((SELECT pn_wgs_affected FROM germline_pn), 0)                                  AS germline_pn_wgs_affected,
    COALESCE(COALESCE(gf.pc_wgs_affected, 0) / NULLIF((SELECT pn_wgs_affected FROM germline_pn), 0), 0)  AS germline_pf_wgs_affected,
    COALESCE(gf.pc_wgs_not_affected, 0)                                                     AS germline_pc_wgs_not_affected,
    COALESCE((SELECT pn_wgs_not_affected FROM germline_pn), 0)                              AS germline_pn_wgs_not_affected,
    COALESCE(COALESCE(gf.pc_wgs_not_affected, 0) / NULLIF((SELECT pn_wgs_not_affected FROM germline_pn), 0), 0) AS germline_pf_wgs_not_affected,
    COALESCE(gf.pc_wxs, 0)                                                                  AS germline_pc_wxs,
    COALESCE((SELECT pn_wxs FROM germline_pn), 0)                                           AS germline_pn_wxs,
    COALESCE(gf.pc_wxs_affected, 0)                                                         AS germline_pc_wxs_affected,
    COALESCE((SELECT pn_wxs_affected FROM germline_pn), 0)                                  AS germline_pn_wxs_affected,
    COALESCE(COALESCE(gf.pc_wxs_affected, 0) / NULLIF((SELECT pn_wxs_affected FROM germline_pn), 0), 0)  AS germline_pf_wxs_affected,
    COALESCE(gf.pc_wxs_not_affected, 0)                                                     AS germline_pc_wxs_not_affected,
    COALESCE((SELECT pn_wxs_not_affected FROM germline_pn), 0)                              AS germline_pn_wxs_not_affected,
    COALESCE(COALESCE(gf.pc_wxs_not_affected, 0) / NULLIF((SELECT pn_wxs_not_affected FROM germline_pn), 0), 0) AS germline_pf_wxs_not_affected,
    COALESCE(sf.pc_tn_wgs, 0)                                                               AS somatic_pc_tn_wgs,
    COALESCE((SELECT pn_tn_wgs FROM somatic_pn), 0)                                         AS somatic_pn_tn_wgs,
    COALESCE(sf.pc_tn_wxs, 0)                                                               AS somatic_pc_tn_wxs,
    COALESCE((SELECT pn_tn_wxs FROM somatic_pn), 0)                                         AS somatic_pn_tn_wxs,
    v.chromosome,
    v.start,
    v.end,
    v.clinvar_name,
    v.variant_class,
    v.clinvar_interpretation,
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
    v.omim_inheritance_code
FROM {{ mapping.starrocks_snv_staging_variant }} v
LEFT JOIN germline_freq gf ON gf.locus_id = v.locus_id
LEFT JOIN somatic_freq sf ON sf.locus_id = v.locus_id
