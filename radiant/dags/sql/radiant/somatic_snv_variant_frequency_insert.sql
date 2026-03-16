INSERT
OVERWRITE {{ mapping.starrocks_somatic_snv_variant_frequency }}
WITH patients_total_count AS (
    SELECT 
    	SUM(pn_tn_wgs) AS pn_tn_wgs,
    	SUM(pn_tn_wxs) AS pn_tn_wxs,
    	SUM(pn_to_wgs) AS pn_to_wgs,
    	SUM(pn_to_wxs) AS pn_to_wxs 
    from (
        SELECT 
        	ANY_VALUE(pn_tn_wgs) AS pn_tn_wgs,
        	ANY_VALUE(pn_tn_wxs) AS pn_tn_wxs,
        	ANY_VALUE(pn_to_wgs) AS pn_to_wgs,
        	ANY_VALUE(pn_to_wxs) AS pn_to_wxs
        FROM radiant.somatic__snv__staging_variant_frequency_part
        GROUP BY part
    ) t       
), 
freq AS (
   SELECT locus_id,
     SUM(pc_tn_wgs) AS pc_tn_wgs,
	 SUM(pc_tn_wxs) AS pc_tn_wxs,
     SUM(pc_to_wgs) AS pc_to_wgs,
     SUM(pc_to_wxs) AS pc_to_wxs
  FROM radiant.somatic__snv__staging_variant_frequency_part
  GROUP BY locus_id
)
SELECT locus_id,
       pc_tn_wgs,
       (SELECT pn_tn_wgs FROM patients_total_count) AS pn_tn_wgs,
       pc_tn_wgs / (SELECT pn_tn_wgs FROM patients_total_count) AS pf_tn_wgs,
       pc_tn_wxs,
       (SELECT pn_tn_wxs FROM patients_total_count) AS pn_tn_wxs,
       pc_tn_wxs / (SELECT pn_tn_wxs FROM patients_total_count) AS pf_tn_wxs,
       pc_to_wgs,
       (SELECT pn_to_wgs FROM patients_total_count) AS pn_to_wgs,
       pc_to_wgs / (SELECT pn_to_wgs FROM patients_total_count) AS pf_to_wgs,
       pc_to_wxs,
       (SELECT pn_to_wxs FROM patients_total_count) AS pn_to_wxs,
       pc_to_wxs / (SELECT pn_to_wxs FROM patients_total_count) AS pf_to_wxs
FROM freq;