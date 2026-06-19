{#
  Genomic coordinate sanity for the Ensembl exon reference:
    - start must not exceed end (1-based inclusive span).
#}

select
    gene_id,
    exon_id,
    chromosome,
    start,
    `end`
from {{ source('radiant', 'ensembl_exon_by_gene') }}
where start > `end`
