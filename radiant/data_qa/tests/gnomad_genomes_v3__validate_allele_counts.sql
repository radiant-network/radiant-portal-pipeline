{#
  Allele-count sanity for the gnomAD frequency table:
    - ac (alt allele count), an (total called alleles) and hom (homozygote
      count) must be non-negative;
    - ac cannot exceed an
#}

select
    locus_id,
    af,
    ac,
    an,
    hom
from {{ source('radiant', 'gnomad_genomes_v3') }}
where ac < 0
   or an < 0
   or hom < 0
   or ac > an
