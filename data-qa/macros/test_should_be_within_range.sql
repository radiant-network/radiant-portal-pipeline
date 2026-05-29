{#
  Generic test — Should Be Within Range, dynamic over columns. Every value
  of the targeted columns must fall in [min_value, max_value]. Columns are
  picked dynamically by `like` substring match (default `_pf_`: the
  frequency/ratio columns), so new matching columns are covered on the next
  run automatically. Returns one row per column that has an out-of-range
  value. NULLs are not flagged (a null ratio is not out of range).
#}

{% test should_be_within_range(model, like='_pf_', min_value=0, max_value=1) %}

    {%- set columns = adapter.get_columns_in_relation(model) -%}
    {%- set checked = [] -%}
    {%- for col in columns if like in col.name -%}
        {%- do checked.append(col.name) -%}
    {%- endfor -%}

    {%- if checked | length == 0 -%}
        select 1 as dummy where 1 = 0
    {%- else -%}
        select column_name, n_out_of_range
        from (
            {%- for col in checked %}
            select
                '{{ col }}' as column_name,
                count(*) as n_out_of_range
            from {{ model }}
            where {{ adapter.quote(col) }} < {{ min_value }}
               or {{ adapter.quote(col) }} > {{ max_value }}
            {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
        ) checks
        where n_out_of_range > 0
    {%- endif -%}

{% endtest %}
