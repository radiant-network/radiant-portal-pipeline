{#
  Occurrences are inserted only when the tumor sample carries the alternate
  allele (`tumor_has_alt`, see somatic_snv_occurrence_insert_partition_delta.sql:
  `WHERE ... AND COALESCE(o.tumor_has_alt, FALSE)`). A WT (0/0) genotype carries
  no alt, so `tumor_zygosity` should never be 'WT' in this table — these rows
  should have been filtered out upstream. ('WT' is still in the accepted-values
  dictionary because it is a valid, portal-handled zygosity in general — this
  asserts the stronger table-specific invariant.)
#}

select
    part,
    task_id,
    tumor_seq_id,
    locus_id,
    tumor_zygosity
from {{ source('radiant', 'somatic__snv__occurrence') }}
where tumor_zygosity = 'WT'
