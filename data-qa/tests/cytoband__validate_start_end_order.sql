{#
  Genomic coordinate sanity for the cytoband reference:
    - start must not exceed end (band span).
#}

select
    chromosome,
    cytoband,
    start,
    `end`
from {{ source('radiant', 'cytoband') }}
where start > `end`
