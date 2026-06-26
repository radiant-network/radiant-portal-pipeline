{#
  Genomic coordinate sanity for germline CNV occurrences:
    - start must not exceed end (1-based inclusive span).
#}

select
    cnv_id,
    chromosome,
    start,
    `end`
from {{ source('radiant', 'germline__cnv__occurrence') }}
where start > `end`
