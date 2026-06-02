{#
  Singular test — Should Be Empty: the aggregated variant table must not
  contain the spanning-deletion placeholder allele ('*'). Such rows are a
  VCF artifact and are expected to be filtered out upstream. A singular
  test passes when this query returns zero rows.
#}

select
    locus_id,
    chromosome,
    start,
    reference,
    alternate
from {{ source('radiant', 'snv__variant') }}
where alternate = '*'
