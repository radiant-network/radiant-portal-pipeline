{#
  SJRA-1820 — `transcript_version` must be populated on RefSeq rows and empty on Ensembl ones.

  The asymmetry is VEP's: it emits no version for Ensembl accessions under any option, and one
  for every RefSeq accession. Pinning it both ways catches the two failure modes of the split in
  radiant/tasks/vcf/snv/consequence.py:

    - a null on a RefSeq row means the version was dropped rather than moved, and the citable
      form (`transcript_id.transcript_version`) can no longer be reconstructed;
    - a value on an Ensembl row means something is being invented, most likely by parsing
      `hgvsc` — which is empty on 30.6% of Ensembl annotations and so cannot fill the column
      consistently anyway.

  Intergenic rows carry no transcript and no source, and are excluded rather than counted as
  either case. That matches resolve_source() rule 4: we record "unknown" rather than guessing.
#}

select
    locus_id,
    symbol,
    transcript_id,
    source,
    transcript_version
from {{ source('radiant', 'snv__consequence') }}
where (source = 'RefSeq' and transcript_version is null)
   or (source = 'Ensembl' and transcript_version is not null)
