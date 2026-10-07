{#
  RAD-22: `hom` counts the carriers called HOM or HEM among the `pc` carriers,
  with the same filters and cohorts, so it can never exceed `pc`. A row here
  means the hom and pc counts drifted apart (filter or cohort mismatch in the
  *_snv_staging_variant_freq_insert.sql queries) or a column swap in the
  positional snv_variant_insert.sql.
#}

select
    locus_id,
    germline_pc_wgs, germline_hom_wgs,
    germline_pc_wxs, germline_hom_wxs,
    somatic_pc_tn_wgs, somatic_hom_tn_wgs,
    somatic_pc_to_wgs, somatic_hom_to_wgs
from {{ source('tenant_db', 'snv__variant') }}
where germline_hom_wgs > germline_pc_wgs
   or germline_hom_wgs_affected > germline_pc_wgs_affected
   or germline_hom_wgs_not_affected > germline_pc_wgs_not_affected
   or germline_hom_wxs > germline_pc_wxs
   or germline_hom_wxs_affected > germline_pc_wxs_affected
   or germline_hom_wxs_not_affected > germline_pc_wxs_not_affected
   or somatic_hom_tn_wgs > somatic_pc_tn_wgs
   or somatic_hom_tn_wxs > somatic_pc_tn_wxs
   or somatic_hom_to_wgs > somatic_pc_to_wgs
   or somatic_hom_to_wxs > somatic_pc_to_wxs
