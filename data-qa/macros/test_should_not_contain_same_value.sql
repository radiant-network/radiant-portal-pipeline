{#
  Generic test — Should Not Contain Same Value, dynamic over columns.

  Introspects the model/source at compile time and asserts that every
  column has more than one distinct (non-null) value. A column where all
  non-null rows share the same value is a strong signal of a broken load
  or a stale derived field.

  count(distinct col) counts non-null distinct values only. A column with
  count(distinct) == 1 has exactly one repeated value across the whole
  table — that's the failure condition.

  Usage in a sources.yml / schema.yml:

    tables:
      - name: snv__variant
        tests:
          - should_not_contain_same_value:
              except: ['locus_id', 'locus']

  When to add to `except`:
    - The column is legitimately constant (e.g. an unused feature flag
      that is always TRUE).
    - The column is guaranteed unique by another test — count(distinct)
      is then expensive and adds no information.

  Schema evolution: new columns are picked up on the next run, dropped
  columns disappear automatically. Only `except` needs maintenance.
#}

{% test should_not_contain_same_value(model, except=[]) %}

    {%- set columns = adapter.get_columns_in_relation(model) -%}
    {%- set checked = columns | rejectattr('name', 'in', except) | list -%}

    {%- if checked | length == 0 -%}
        select 1 as dummy where 1 = 0
    {%- else -%}
        select column_name, n_distinct
        from (
            {%- for col in checked %}
            select
                '{{ col.name }}' as column_name,
                count(distinct {{ adapter.quote(col.name) }}) as n_distinct
            from {{ model }}
            {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
        ) checks
        where n_distinct = 1
    {%- endif -%}

{% endtest %}
