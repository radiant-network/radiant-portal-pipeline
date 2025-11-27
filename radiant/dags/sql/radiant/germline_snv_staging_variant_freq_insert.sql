INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_germline_snv_staging_variant_frequency }}
WITH patients_total_count
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
             FROM {{ mapping.starrocks_staging_sequencing_experiment }} s
             where s.seq_id in (select seq_id from  {{ mapping.starrocks_germline_snv_occurrence }} where part = %(part)s)),
     freqs as (SELECT o.part,
                      o.locus_id,
                      COUNT(distinct CASE WHEN s.experimental_strategy = 'wgs' then patient_id end)                   AS pc_wgs,
                      COUNT(distinct CASE
                                         WHEN s.experimental_strategy = 'wgs' and s.affected_status = 'affected'
                                             then patient_id end)                                                  AS pc_wgs_affected,
                      COUNT(distinct CASE
                                         WHEN s.experimental_strategy = 'wgs' and s.affected_status = 'non_affected'
                                             then patient_id end)                                                  AS pc_wgs_not_affected,
                      COUNT(distinct CASE WHEN s.experimental_strategy = 'wxs' then patient_id end)                        AS pc_wxs,
                      COUNT(distinct CASE
                                         WHEN s.experimental_strategy = 'wxs' and s.affected_status = 'affected'
                                             then patient_id end)                                                  AS pc_wxs_affected,
                      COUNT(distinct CASE
                                         WHEN s.experimental_strategy = 'wxs' and s.affected_status = 'non_affected'
                                             then patient_id end)                                                  AS pc_wxs_not_affected
               FROM  {{ mapping.starrocks_germline_snv_occurrence }} o
                        JOIN {{ mapping.starrocks_staging_sequencing_experiment }} s ON s.seq_id = o.seq_id
               WHERE o.part = %(part)s
                 AND o.gq >= 20 AND o.filter='PASS' AND o.ad_alt > 3
               GROUP BY locus_id, o.part)
SELECT part,
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
       (SELECT cnt_wxs_not_affected FROM patients_total_count) AS pn_wxs_not_affected
from freqs