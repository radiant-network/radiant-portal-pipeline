{#
  `pc` counts the patients carrying a matching CNV, `pn` the patients with any
  tumor-only CNV occurrence, so `pc` can never exceed `pn`. A row here means a
  cohort mismatch between the counts and the denominators in
  somatic_cnv_variant_frequency_insert.sql.
#}

select cnv_id, pc_to_wgs, pn_to_wgs, pc_to_wxs, pn_to_wxs
from {{ source('tenant_db', 'somatic__cnv__variant_frequency') }}
where pc_to_wgs > pn_to_wgs
   or pc_to_wxs > pn_to_wxs
