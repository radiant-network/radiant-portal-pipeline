{#
  SQL condition: the tenant of the current pass has a non-deleted task of `task_type` in
  staging_sequencing_experiment — the same criterion import_part gates each load on (e.g.
  somatic CNVs are only loaded for `tumor_only_variant_calling` tasks).

  Meant for the `expected_if` option of should_not_be_empty / should_not_contain_only_null:
  a table that the tenant's tasks never fill is allowed to be empty, and the test turns strict
  as soon as such a task is declared.

  The tenant code comes from SR_TENANT_CODE, set by run_qa.py on each tenant pass. Without it
  (plain local run, shared pass) the condition is `true`, so the test keeps its strict form.
#}

{% macro tenant_has_task_type(task_type) %}
  {%- set tenant_code = env_var('SR_TENANT_CODE', '') -%}
  {%- if tenant_code -%}
    exists (
        select 1
        from {{ adapter.quote(env_var('SR_SCHEMA', 'radiant')) }}.staging_sequencing_experiment
        where tenant_code = '{{ tenant_code }}'
          and task_type = '{{ task_type }}'
          and not deleted
    )
  {%- else -%}
    true
  {%- endif -%}
{% endmacro %}
