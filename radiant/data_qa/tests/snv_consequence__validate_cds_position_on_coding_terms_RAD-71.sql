{#
  RAD-71 — a consequence with a coding term has a CDS position.

  `cds_start` and `cds_end` are copied from VEP's `CDS_position`, which VEP fills whenever the variant
  overlaps the coding sequence. An indel partly outside the CDS keeps one end only (`?-125`), so the rule is
  that at least one of the two is set.

  A failure means the CSQ lookup lost `CDS_position`, or the rows predate the RAD-71 re-import: the migration
  adds the columns with NULL on every existing row, and only a re-import fills them. Expect this test to fail
  in an environment until its re-import is done.

  The `_RAD-71` suffix makes those failures known errors, so they do not fail the data integrity run during the
  backfill. Drop it once every environment is re-imported, so the test starts guarding the CSQ lookup.
#}

select
    locus_id,
    symbol,
    transcript_id,
    consequences
from {{ source('radiant', 'snv__consequence') }}
where cds_start is null
  and cds_end is null
  and arrays_overlap(consequences, [
      'missense_variant', 'synonymous_variant', 'stop_gained', 'stop_lost', 'stop_retained_variant',
      'start_lost', 'start_retained_variant', 'frameshift_variant', 'inframe_insertion', 'inframe_deletion',
      'protein_altering_variant', 'coding_sequence_variant', 'incomplete_terminal_codon_variant'
  ])
