{#
  RAD-175 — the share of RefSeq MANE Select rows whose `mane_pair_transcript_id` names no Ensembl
  row at the same locus stays under 1%.

  Some never resolve, and not through the ETL: in VEP's merged cache the RefSeq half's MANE_SELECT
  follows a newer MANE release than the Ensembl half, so for a few genes it names an ENST the
  Ensembl half does not have yet (cache 114: 20 transcripts — MUC16, FCGBP, NBPF1, MUC2… — 0.1% of
  the rows in QA). A share well above that means --mane was lost upstream or the two halves come
  from different caches. Returns one row, with the figures, when the threshold is crossed.
#}

with refseq_mane as (
    select
        r.locus_id,
        r.mane_pair_transcript_id,
        e.locus_id as twin_locus_id
    from {{ source('radiant', 'snv__consequence') }} r
    left join (
        select distinct locus_id, transcript_id
        from {{ source('radiant', 'snv__consequence') }}
        where source = 'Ensembl'
          and transcript_id <> ''
    ) e
        on e.locus_id = r.locus_id
       and e.transcript_id = r.mane_pair_transcript_id
    where r.source = 'RefSeq'
      and r.is_mane_select
      and nullif(r.mane_pair_transcript_id, '') is not null
)

select
    count(*) as refseq_mane_rows,
    sum(case when twin_locus_id is null then 1 else 0 end) as unresolved_rows,
    count(distinct case when twin_locus_id is null then mane_pair_transcript_id end) as unresolved_transcripts
from refseq_mane
having sum(case when twin_locus_id is null then 1 else 0 end) > 0.01 * count(*)
