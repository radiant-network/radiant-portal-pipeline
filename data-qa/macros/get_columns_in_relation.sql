{#
  Project-level override of the dbt-starrocks adapter macro.

  The stock starrocks__get_columns_in_relation runs a `desc <table>`
  statement to resolve inner types of array/struct/map columns. StarRocks
  forbids `desc` inside an explicit transaction (error 5305 / 25P01), and
  dbt's `statement` macro opens one via auto_begin=True. Our data-QA tests
  only need column NAMES, so we drop the `desc` query and build columns from
  INFORMATION_SCHEMA.columns alone (a plain SELECT, allowed in a transaction).
#}
{% macro starrocks__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale
    from INFORMATION_SCHEMA.columns
    where table_name = '{{ relation.identifier }}'
      {% if relation.schema %}
      and table_schema = '{{ relation.schema }}'
      {% endif %}
    order by ordinal_position
  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {% set columns = [] %}
  {% for row in table %}
    {% do columns.append(api.Column(*row)) %}
  {% endfor %}
  {{ return(columns) }}
{% endmacro %}
