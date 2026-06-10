{#
  Allele-count sanity for the 1000 Genomes frequency table:
    - ac (alt allele count) and an (total called alleles) must be non-negative;
    - ac cannot exceed an
#}

select
    locus_id,
    af,
    ac,
    an
from {{ source('radiant', '1000_genomes') }}
where ac < 0
   or an < 0
   or ac > an
