{#
  `pc` counts the patients carrying a matching CNV, `pn` the patients with any
  CNV occurrence of the same cohort, so `pc` can never exceed `pn`. A row here
  means a cohort mismatch between the counts and the denominators in
  germline_cnv_variant_frequency_insert.sql.
#}

select cnv_id, pc_wgs, pn_wgs, pc_wxs, pn_wxs
from {{ source('tenant_db', 'germline__cnv__variant_frequency') }}
where pc_wgs > pn_wgs
   or pc_wgs_affected > pn_wgs_affected
   or pc_wgs_not_affected > pn_wgs_not_affected
   or pc_wxs > pn_wxs
   or pc_wxs_affected > pn_wxs_affected
   or pc_wxs_not_affected > pn_wxs_not_affected
