{#
  Occurrences are inserted only when the sample carries the alternate allele
  (`has_alt = 1 in calls`, see germline_snv_occurrence_insert_partition_delta.sql:
  `WHERE ... AND has_alt`). A WT (0/0) genotype carries no alt, so `zygosity`
  should never be 'WT' in this table — these rows should have been filtered out
  upstream. ('WT' is still in the accepted-values dictionary because it is a
  valid, portal-handled zygosity in general — this asserts the stronger
  table-specific invariant.)
#}

select
    part,
    seq_id,
    task_id,
    locus_id,
    zygosity
from {{ source('radiant', 'germline__snv__occurrence') }}
where zygosity = 'WT'
