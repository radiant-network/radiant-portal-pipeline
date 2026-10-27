{#
  RAD-71 — a consequence that never reaches the coding sequence has no CDS position.

  Only rows whose every term is intronic, UTR, up/downstream or intergenic are checked: VEP leaves
  `CDS_position` empty on those. A value there means a column was read from the wrong CSQ index, or the
  positional INSERT shifted a neighbour into `cds_start` / `cds_end`.

  "Every term is in the list" is written as an intersection-size comparison: StarRocks 3.4 fails a lambda
  (`array_filter(consequences, x -> ...)`) in this WHERE with "missing statistic of col: auto_fill_col".
#}

select
    locus_id,
    symbol,
    transcript_id,
    consequences,
    cds_start,
    cds_end
from {{ source('radiant', 'snv__consequence') }}
where (cds_start is not null or cds_end is not null)
  and array_length(array_intersect(consequences, [
      'intron_variant', '3_prime_UTR_variant', '5_prime_UTR_variant',
      'upstream_gene_variant', 'downstream_gene_variant', 'intergenic_variant'
  ])) = array_length(array_distinct(consequences))
