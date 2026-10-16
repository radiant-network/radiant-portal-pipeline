{#
  A table declared absent for this tenant (`absent_tables`, TENANT_QA_SHAPES in
  the data_integrity_starrocks DAG) must hold no rows. A row means the tenant's
  shape changed — e.g. its first somatic analysis was imported — and the entry
  is out of date: every test on that table is skipped until it is removed.

  The tables are read by name, not through `source()`: run_qa.py excludes the
  absent tables' nodes from the pass, and a `source()` dependency would exclude
  this test along with them. The `depends_on` below only places the test in the
  tenant pass; `snv__variant` is never declared absent.
#}

{{ config(tags=['tenant_health']) }}

-- depends_on: {{ source('tenant_db', 'snv__variant') }}

{%- set schema = env_var('SR_TENANT_SCHEMA', env_var('SR_SCHEMA', 'radiant')) -%}
{%- set absent_tables = var('absent_tables', []) -%}

{%- if absent_tables | length == 0 %}
select 1 as dummy where 1 = 0
{%- else %}
select absent_table, n_rows
from (
    {%- for table in absent_tables %}
    select '{{ table }}' as absent_table, count(*) as n_rows
    from {{ adapter.quote(schema) }}.{{ adapter.quote(table) }}
    {%- if not loop.last %} union all {% endif %}
    {%- endfor %}
) counts
where n_rows > 0
{%- endif %}
