{#
  Every Ensembl MANE Select transcript has its RefSeq id. The id is matched on the Ensembl transcript version,
  so a row here means the MANE release does not match the Ensembl release (MANE 1.4 is built on Ensembl 114).
#}

select
    ensembl_gene_id,
    ensembl_transcript_id,
    ensembl_transcript_version
from {{ source('radiant', 'gene_reference_transcript') }}
where is_mane_select
  and refseq_transcript_id is null
