{#
  Generic test — Should Not Contain Only Null, dynamic over columns.

  Introspects the model/source at compile time and asserts that every
  column has at least one non-null value. A column where every row is
  null is almost always a load bug or a stale derived field.

  Usage in a sources.yml / schema.yml:

    tables:
      - name: snv__variant
        tests:
          - should_not_contain_only_null:
              except: ['locus_id', 'chromosome', 'start', 'reference',
                       'alternate', 'locus']

  When to add to `except`:
    - The column is already covered by a stricter `not_null` test — no
      reason to re-check it via the dynamic sweep.
    - The column is legitimately allowed to be entirely null in some
      environments (e.g. a feature gated by a flag that is off in QA).

  `expected_if` (optional): as for should_not_be_empty, a SQL condition
  saying whether the tenant is expected to fill the table. An empty table
  makes every column null, so a table the tenant never fills needs it too.

  Schema evolution: new columns are picked up on the next run, dropped
  columns disappear automatically. Only `except` needs maintenance.
#}

{% test should_not_contain_only_null(model, except=[], expected_if=none) %}

    {%- set columns = adapter.get_columns_in_relation(model) -%}
    {%- set checked = columns | rejectattr('name', 'in', except) | list -%}

    {%- if checked | length == 0 -%}
        select 1 as dummy where 1 = 0
    {%- else -%}
        select column_name, n_non_null
        from (
            {%- for col in checked %}
            select
                '{{ col.name }}' as column_name,
                count({{ adapter.quote(col.name) }}) as n_non_null
            from {{ model }}
            {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
        ) checks
        where n_non_null = 0
        {%- if expected_if %}
          and {{ expected_if }}
        {%- endif %}
    {%- endif -%}

{% endtest %}
