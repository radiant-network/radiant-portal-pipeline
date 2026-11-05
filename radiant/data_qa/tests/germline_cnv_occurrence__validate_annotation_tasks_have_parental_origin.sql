{#
  The slivar columns of germline__cnv__occurrence are exempt from should_not_contain_only_null: a
  tenant with no `radiant_germline_cnv_annotation` task has none of them. This test guards them
  where they must be filled: a non-deleted annotation task whose staging rows include a parent
  (`family_role` mother or father) gets its `po_*` tags from slivar, so at least one of its
  occurrences must carry a `parental_origin`. A task with occurrences and none at all is the load
  defect the seeds name (`seeds.sql`, document 284): a column-only reheader of the slivar VCF, whose
  INFO tags then list sample names the ETL does not know, leaves every parental origin and
  transmission mode NULL. `transmission_mode` is not guarded: an unaffected proband has an origin but
  no mode (RAD-169 spec, Empty values).

  Gated on staging like germline_cnv_occurrence__validate_annotation_rows_are_proband.sql;
  staging_sequencing_experiment lives in the base database (SR_SCHEMA). Tagged tenant_health: the
  defect is per tenant VCF, so every tenant's annotation tasks are checked, not the reference tenant's only.
#}

{{ config(tags=['tenant_health']) }}

with annotation_tasks_with_a_parent as (
    select distinct task_id
    from {{ adapter.quote(env_var('SR_SCHEMA', 'radiant')) }}.staging_sequencing_experiment
    where task_type = 'radiant_germline_cnv_annotation'
      and not deleted
      and family_role in ('mother', 'father')
)

select
    o.task_id,
    count(1) as occurrences
from {{ source('tenant_db', 'germline__cnv__occurrence') }} o
join {{ adapter.quote(env_var('SR_SCHEMA', 'radiant')) }}.staging_sequencing_experiment s
    on s.task_id = o.task_id
   and s.seq_id = o.seq_id
join annotation_tasks_with_a_parent p
    on p.task_id = o.task_id
where s.task_type = 'radiant_germline_cnv_annotation'
  and not s.deleted
group by o.task_id
having count(o.parental_origin) = 0
