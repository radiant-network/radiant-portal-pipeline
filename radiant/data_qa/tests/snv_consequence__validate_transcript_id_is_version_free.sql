{#
  SJRA-1820 — `transcript_id` must never carry a version suffix, on either catalogue.

  `snv__consequence` is PRIMARY KEY(locus_id, symbol, transcript_id). VEP emits RefSeq
  accessions versioned (`NM_000546.6`) and Ensembl ones bare, so storing `Feature` as is would
  put the annotation cache release inside row identity for one catalogue only: a cohort
  annotated against a newer RefSeq release arrives as `NM_000546.7`, a different key, and lands
  beside the old row instead of replacing it. The variant page then lists one transcript twice
  with no way to tell which is current.

  Extraction splits the accession (radiant/tasks/vcf/snv/consequence.py), so a hit here means
  either the split was bypassed or rows predating SJRA-1820 survived the migration. Both fail
  silently otherwise — the duplicate rows are individually well-formed.

  The companion assertion, that the version is not simply lost, lives in
  snv_consequence__validate_transcript_version_matches_source.sql.
#}

select
    locus_id,
    symbol,
    transcript_id,
    source
from {{ source('radiant', 'snv__consequence') }}
where transcript_id like '%.%'
