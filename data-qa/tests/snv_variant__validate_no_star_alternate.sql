{#
  `*` in `alternate` is the spanning-deletion placeholder (VCF artifact),
  expected to be filtered out upstream — this table should contain none.
#}

select
    locus_id,
    chromosome,
    start,
    reference,
    alternate
from {{ source('radiant', 'snv__variant') }}
where alternate = '*'
