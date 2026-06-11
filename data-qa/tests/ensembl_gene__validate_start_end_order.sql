{#
  Genomic coordinate sanity for the Ensembl gene reference:
    - start must not exceed end (1-based inclusive span).
#}

select
    gene_id,
    chromosome,
    start,
    `end`
from {{ source('radiant', 'ensembl_gene') }}
where start > `end`
