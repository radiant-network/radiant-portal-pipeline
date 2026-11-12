{#
  SQL condition: the tenant of the current pass has loaded CNV Exomiser rows (RAD-181) in raw_exomiser_cnv, the shared
  table `load_exomiser_cnv_files` fills and the tenant `exomiser_cnv` insert reads. The raw table holds only the
  contributing rows of the reports `import_part` has loaded. A report in staging_sequencing_experiment is not enough:
  it can wait for its import, or have no contributing row (a small CNV set), and the tenant table then stays empty
  with no defect.

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
        from {{ adapter.quote(env_var('SR_SCHEMA', 'radiant')) }}.raw_exomiser_cnv
        where tenant_code = '{{ tenant_code }}'
    )
  {%- else -%}
    true
  {%- endif -%}
{% endmacro %}
