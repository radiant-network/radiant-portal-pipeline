{#
  SQL condition: the tenant of the current pass has a non-deleted `radiant_germline_cnv_annotation` task with a CNV
  Exomiser report (RAD-181) in staging_sequencing_experiment, the rows `load_exomiser_cnv_files` loads. A CNV annotation
  task alone is not enough: CNV post-processing runs Exomiser only for a family with a phenopacket.

  Meant for the `expected_if` option of should_not_be_empty / should_not_contain_only_null on `exomiser_cnv`, as
  tenant_has_task_type is for the somatic tables.

  The tenant code comes from SR_TENANT_CODE, set by run_qa.py on each tenant pass. Without it
  (plain local run, shared pass) the condition is `true`, so the test keeps its strict form.
#}
{% macro tenant_has_cnv_exomiser_report() %}
  {%- set tenant_code = env_var('SR_TENANT_CODE', '') -%}
  {%- if tenant_code -%}
    exists (
        select 1
        from {{ adapter.quote(env_var('SR_SCHEMA', 'radiant')) }}.staging_sequencing_experiment
        where tenant_code = '{{ tenant_code }}'
          and task_type = 'radiant_germline_cnv_annotation'
          and exomiser_filepath is not null
          and not deleted
    )
  {%- else -%}
    true
  {%- endif -%}
{% endmacro %}
