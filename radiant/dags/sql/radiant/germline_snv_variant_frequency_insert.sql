INSERT
OVERWRITE {{ mapping.starrocks_germline_snv_variant_frequency }}
WITH patients_total_count AS (
    SELECT SUM(pn_wgs) AS pn_wgs, SUM(pn_wgs_affected) AS pn_wgs_affected, SUM(pn_wgs_not_affected) AS pn_wgs_not_affected, SUM(pn_wxs) AS pn_wxs, SUM(pn_wxs_affected) AS pn_wxs_affected, SUM(pn_wxs_not_affected) AS pn_wxs_not_affected
    from (
        SELECT ANY_VALUE(pn_wgs) AS pn_wgs,
            ANY_VALUE(pn_wgs_affected) AS pn_wgs_affected,
            ANY_VALUE(pn_wgs_not_affected) AS pn_wgs_not_affected,
            ANY_VALUE(pn_wxs) AS pn_wxs,
            ANY_VALUE(pn_wxs_affected) AS pn_wxs_affected,
            ANY_VALUE(pn_wxs_not_affected) AS pn_wxs_not_affected
        FROM {{ mapping.starrocks_germline_snv_staging_variant_frequency }}
        GROUP BY part
    ) t       
), 
freq AS (
   SELECT locus_id,
                 SUM(pc_wgs)                 AS pc_wgs,
                 SUM(pc_wgs_affected)     AS pc_wgs_affected,
                 SUM(pc_wgs_not_affected) AS pc_wgs_not_affected,
                 SUM(pc_wxs)                 AS pc_wxs,
                 SUM(pc_wxs_affected)     AS pc_wxs_affected,
                 SUM(pc_wxs_not_affected) AS pc_wxs_not_affected
          FROM {{ mapping.starrocks_germline_snv_staging_variant_frequency }}
          GROUP BY locus_id
)
SELECT locus_id,
       pc_wgs,
       (SELECT pn_wgs FROM patients_total_count)                                    AS pn_wgs,
       pc_wgs / (SELECT pn_wgs FROM patients_total_count)                           AS pf_wgs,
       pc_wgs_affected,
       (SELECT pn_wgs_affected FROM patients_total_count)                           AS pn_wgs_affected,
       pc_wgs_affected / (SELECT pn_wgs_affected FROM patients_total_count)         AS pf_wgs_affected,
       pc_wgs_not_affected,
       (SELECT pn_wgs_not_affected FROM patients_total_count)                       AS pn_wgs_not_affected,
       pc_wgs_not_affected / (SELECT pn_wgs_not_affected FROM patients_total_count) AS pf_wgs_not_affected,
       pc_wxs,
       (SELECT pn_wxs FROM patients_total_count)                                    AS pn_wxs,
       pc_wxs / (SELECT pn_wxs FROM patients_total_count)                           AS pf_wxs,
       pc_wxs_affected,
       (SELECT pn_wxs_affected FROM patients_total_count)                           AS pn_wxs_affected,
       pc_wxs_affected / (SELECT pn_wxs_affected FROM patients_total_count)         AS pf_wxs_affected,
       pc_wxs_not_affected,
       (SELECT pn_wxs_not_affected FROM patients_total_count)                       AS pn_wxs_not_affected,
       pc_wxs_not_affected / (SELECT pn_wxs_not_affected FROM patients_total_count) AS pf_wxs_not_affected
FROM freq