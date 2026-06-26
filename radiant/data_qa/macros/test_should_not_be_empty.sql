{#
  Generic test — Should Not Be Empty. Fails when the relation has zero
  rows: a silently empty source table means an upstream load dropped 
  or never produced data.

  On an empty relation `count(*)` is 0, the `having` keeps that single
  aggregate row, and the test reports one failing row. On a populated
  relation the `having` filters it out and the test passes.

  Usage in a sources.yml / schema.yml:

    tables:
      - name: snv__variant
        tests:
          - should_not_be_empty:
              name: snv_variant__should_not_be_empty
#}

{% test should_not_be_empty(model) %}

    select count(*) as n_rows
    from {{ model }}
    having count(*) = 0

{% endtest %}
