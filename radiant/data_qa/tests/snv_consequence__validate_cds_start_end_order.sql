{#
  RAD-71 — `cds_start` must not exceed `cds_end`.

  VEP writes `CDS_position` lowest position first on both strands, so the split must keep that order. Rows
  with one end NULL (an indel partly outside the CDS) are not compared.
#}

select
    locus_id,
    symbol,
    transcript_id,
    cds_start,
    cds_end
from {{ source('radiant', 'snv__consequence') }}
where cds_start > cds_end
