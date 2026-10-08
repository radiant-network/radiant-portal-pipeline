INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_germline_snv_staging_variant_frequency }}
WITH germline_sequencings AS (
    SELECT * FROM {{ mapping.starrocks_staging_sequencing_experiment }} s
    WHERE s.analysis_type = 'germline'
      AND s.tenant_code = %(tenant_code)s
      AND s.seq_id in (select seq_id from  {{ mapping.starrocks_germline_snv_occurrence }} where part = %(part)s)
), patients_total_count
     AS (SELECT COUNT(DISTINCT CASE WHEN s.experimental_strategy = 'wgs' then s.patient_id end)                   AS cnt_wgs,
                COUNT(DISTINCT CASE
                                   WHEN s.experimental_strategy = 'wgs' and s.affected_status = 'affected'
                                       then s.patient_id end)                                                  AS cnt_wgs_affected,
                COUNT(DISTINCT CASE
                                   WHEN s.experimental_strategy = 'wgs' and s.affected_status = 'non_affected'
                                       then s.patient_id end)                                                  AS cnt_wgs_not_affected,
                COUNT(DISTINCT CASE WHEN s.experimental_strategy = 'wxs' then s.patient_id end)                        AS cnt_wxs,
                COUNT(DISTINCT CASE
                                   WHEN s.experimental_strategy = 'wxs' and s.affected_status = 'affected'
                                       then s.patient_id end)                                                  AS cnt_wxs_affected,
                COUNT(DISTINCT CASE
                                   WHEN s.experimental_strategy = 'wxs' and s.affected_status = 'non_affected'
                                       then s.patient_id end)                                                  AS cnt_wxs_not_affected
         FROM germline_sequencings s
),
-- RAD-57: every occurrence of the part, flagged instead of filtered. Filtering here dropped the loci whose
-- occurrences all fail the quality gate from the frequency table, hence from `tenant_loci` in
-- snv_variant_insert.sql, hence from the case. They must stay, with zero counts. NULL `gq` or `ad_alt`
-- leaves `qualifies` NULL, which counts nowhere.
occurrences as (SELECT o.part,
                       o.locus_id,
                       o.seq_id,
                       o.zygosity,
                       o.gq >= 20 AND o.filter = 'PASS' AND o.ad_alt >= 3 AS qualifies
                FROM {{ mapping.starrocks_germline_snv_occurrence }} o
                WHERE o.part = %(part)s
), freqs as (SELECT o.part,
                  o.locus_id,
                  COUNT(distinct CASE WHEN o.qualifies and s.experimental_strategy = 'wgs' then patient_id end)                   AS pc_wgs,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wgs' and s.affected_status = 'affected'
                                         then patient_id end)                                                  AS pc_wgs_affected,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wgs' and s.affected_status = 'non_affected'
                                         then patient_id end)                                                  AS pc_wgs_not_affected,
                  COUNT(distinct CASE WHEN o.qualifies and s.experimental_strategy = 'wxs' then patient_id end)                        AS pc_wxs,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wxs' and s.affected_status = 'affected'
                                         then patient_id end)                                                  AS pc_wxs_affected,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wxs' and s.affected_status = 'non_affected'
                                         then patient_id end)                                                  AS pc_wxs_not_affected,
                  -- RAD-22: carriers called homozygous, hemizygous included. Same gate and cohorts as pc_*, and
                  -- distinct per patient, so a patient HOM in one sample and HET in another counts once here.
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wgs' and o.zygosity IN ('HOM', 'HEM')
                                         then patient_id end)                                                  AS hom_wgs,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wgs' and s.affected_status = 'affected' and o.zygosity IN ('HOM', 'HEM')
                                         then patient_id end)                                                  AS hom_wgs_affected,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wgs' and s.affected_status = 'non_affected' and o.zygosity IN ('HOM', 'HEM')
                                         then patient_id end)                                                  AS hom_wgs_not_affected,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wxs' and o.zygosity IN ('HOM', 'HEM')
                                         then patient_id end)                                                  AS hom_wxs,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wxs' and s.affected_status = 'affected' and o.zygosity IN ('HOM', 'HEM')
                                         then patient_id end)                                                  AS hom_wxs_affected,
                  COUNT(distinct CASE
                                     WHEN o.qualifies and s.experimental_strategy = 'wxs' and s.affected_status = 'non_affected' and o.zygosity IN ('HOM', 'HEM')
                                         then patient_id end)                                                  AS hom_wxs_not_affected
			FROM  occurrences o
			JOIN {{ mapping.starrocks_staging_sequencing_experiment }} s ON s.seq_id = o.seq_id
           WHERE s.tenant_code = %(tenant_code)s
           GROUP BY locus_id, o.part
)
SELECT %(tenant_code)s AS tenant_code,
       part,
       locus_id,
       pc_wgs,
       (SELECT cnt_wgs FROM patients_total_count)                 AS pn_wgs,
       pc_wgs_affected,
       (SELECT cnt_wgs_affected FROM patients_total_count)     AS pn_wgs_affected,
       pc_wgs_not_affected,
       (SELECT cnt_wgs_not_affected FROM patients_total_count) AS pn_wgs_not_affected,
       pc_wxs,
       (SELECT cnt_wxs FROM patients_total_count)                 AS pn_wxs,
       pc_wxs_affected,
       (SELECT cnt_wxs_affected FROM patients_total_count)     AS pn_wxs_affected,
       pc_wxs_not_affected,
       (SELECT cnt_wxs_not_affected FROM patients_total_count) AS pn_wxs_not_affected,
       hom_wgs,
       hom_wgs_affected,
       hom_wgs_not_affected,
       hom_wxs,
       hom_wxs_affected,
       hom_wxs_not_affected
from freqs