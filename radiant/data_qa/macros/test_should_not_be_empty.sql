{#
  Generic test — Should Not Be Empty. Fails when the relation has zero
  rows: a silently empty source table means an upstream load dropped 
  or never produced data.

  On an empty relation `count(*)` is 0, the `where` keeps that single
  aggregate row, and the test reports one failing row. On a populated
  relation the `where` filters it out and the test passes.

  `expected_if` (optional): a SQL condition saying whether the tenant is
  expected to fill the table, e.g. `tenant_has_task_type(...)`. When it is
  false the empty table passes.

  Usage in a sources.yml / schema.yml:

    tables:
      - name: snv__variant
        tests:
          - should_not_be_empty:
              name: snv_variant__should_not_be_empty
          - should_not_be_empty:
              name: somatic_cnv_occurrence__should_not_be_empty
              expected_if: "{{ tenant_has_task_type('tumor_only_variant_calling') }}"
#}

{% test should_not_be_empty(model, expected_if=none) %}

    select n_rows
    from (
        select count(*) as n_rows
        from {{ model }}
    ) counts
    where n_rows = 0
    {%- if expected_if %}
      and {{ expected_if }}
    {%- endif %}

{% endtest %}
