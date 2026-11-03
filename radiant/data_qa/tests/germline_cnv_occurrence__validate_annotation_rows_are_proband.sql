{#
  The slivar CNV set is loaded for the proband only. Every germline CNV occurrence of a
  `radiant_germline_cnv_annotation` task must belong to a sequencing that is the case's proband in that
  task's staging rows. A parent's or a sibling's row means the ETL loaded a relative's slivar records.

  staging_sequencing_experiment lives in the base database (SR_SCHEMA), as in macros/tenant_has_task_type.sql.
#}

select
    o.task_id,
    o.seq_id,
    count(1) as occurrences
from {{ source('tenant_db', 'germline__cnv__occurrence') }} o
join {{ adapter.quote(env_var('SR_SCHEMA', 'radiant')) }}.staging_sequencing_experiment s
    on s.task_id = o.task_id
   and s.seq_id = o.seq_id
where s.task_type = 'radiant_germline_cnv_annotation'
  and not s.deleted
  and s.family_role <> 'proband'
group by o.task_id, o.seq_id
