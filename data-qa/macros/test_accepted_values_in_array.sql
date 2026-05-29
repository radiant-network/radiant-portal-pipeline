{#
  Generic test — Accepted Values for an ARRAY column. The built-in
  `accepted_values` only handles scalars; this unnests the array and
  flags any element not in `values` (raw, case-sensitive). Returns one
  row per distinct unknown value.
#}

{% test accepted_values_in_array(model, column_name, values) %}

select distinct
    val as invalid_value
from {{ model }} s,
     unnest(s.{{ column_name }}) as t(val)
where val is not null
  and val not in (
    {%- for v in values %}
    '{{ v }}'{{ "," if not loop.last }}
    {%- endfor %}
  )

{% endtest %}
