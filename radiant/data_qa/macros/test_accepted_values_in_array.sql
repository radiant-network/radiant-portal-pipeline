{#
  Generic test — Accepted Values for an ARRAY column. The built-in
  `accepted_values` only handles scalars; this flags any element not in
  `values` (raw, case-sensitive). Returns one row per distinct unknown
  value.
#}

{% test accepted_values_in_array(model, column_name, values) %}

select distinct
    val as invalid_value
from {{ model }} s,
     unnest(array_filter(
       s.{{ column_name }},
       x -> x is not null and not array_contains([
         {%- for v in values %}'{{ v }}'{{ "," if not loop.last }}{%- endfor %}
       ], x)
     )) as t(val)

{% endtest %}
