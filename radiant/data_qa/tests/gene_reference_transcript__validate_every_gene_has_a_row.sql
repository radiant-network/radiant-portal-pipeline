{#
  Every Ensembl gene has a reference transcript. A gene without one gets no exon count in the portal: a row
  here means an Ensembl release has a gene with neither a MANE Select nor a canonical transcript.
#}

select
    g.gene_id
from {{ source('radiant', 'ensembl_gene') }} g
left join {{ source('radiant', 'gene_reference_transcript') }} r
       on r.ensembl_gene_id = g.gene_id
where r.ensembl_gene_id is null
