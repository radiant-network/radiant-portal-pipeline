{#
  SJRA-1830 — only rule 4 of resolve_source() (radiant/tasks/vcf/snv/consequence.py) leaves
  `source` empty, and it fires on intergenic rows alone. A null source on a row that has a
  transcript means rules 1-3 all missed — a broken CSQ lookup or an unrecognised namespace.
#}

select
    locus_id,
    symbol,
    transcript_id,
    transcript_version
from {{ source('radiant', 'snv__consequence') }}
where source is null
  and transcript_id <> ''
