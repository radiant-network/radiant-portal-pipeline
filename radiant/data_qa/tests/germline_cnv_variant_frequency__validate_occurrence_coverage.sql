{#
  Every interval of the occurrences gets a frequency row, also when none of
  its CNVs passes the filter. With the uniqueness of `cnv_id` on the frequency
  table, each occurrence joins to exactly one row. A row here means the
  frequency table is older than the occurrences (the recompute after the
  occurrence insert failed or was skipped) or the insert dropped intervals.
#}

{{ config(tags=['tenant_health']) }}

select distinct o.cnv_id
from {{ source('tenant_db', 'germline__cnv__occurrence') }} o
left join {{ source('tenant_db', 'germline__cnv__variant_frequency') }} f
    on f.cnv_id = o.cnv_id
where f.cnv_id is null
