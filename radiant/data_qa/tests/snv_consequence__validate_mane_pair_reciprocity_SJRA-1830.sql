{#
  SJRA-1830 — a RefSeq MANE Select row's `mane_pair_transcript_id` must resolve to an Ensembl row
  at the same locus that is itself MANE Select and points back. SJRA-1827's score borrow joins on
  that pointer, so a wrong one silently reads another transcript's scores.

  RefSeq -> Ensembl only: the reverse is red table-wide, since rows from pre-merged loads point at
  RefSeq transcripts that were never ingested. MANE Plus Clinical is excluded — VEP fills
  MANE_SELECT only on the Select transcript. The twin is matched on transcript rather than symbol,
  which is in the primary key but not stable across annotation-cache versions.
#}

with ensembl_twin as (
    select
        locus_id,
        transcript_id,
        max(cast(is_mane_select as int)) as twin_is_mane_select,
        max(mane_pair_transcript_id) as twin_pair_transcript_id
    from {{ source('radiant', 'snv__consequence') }}
    where source = 'Ensembl'
      and transcript_id <> ''
    group by locus_id, transcript_id
)

select
    r.locus_id,
    r.symbol,
    r.transcript_id,
    r.mane_pair_transcript_id,
    t.twin_is_mane_select,
    t.twin_pair_transcript_id
from {{ source('radiant', 'snv__consequence') }} r
left join ensembl_twin t
    on t.locus_id = r.locus_id
   and t.transcript_id = r.mane_pair_transcript_id
where r.source = 'RefSeq'
  and r.is_mane_select
  and nullif(r.mane_pair_transcript_id, '') is not null
  and (t.locus_id is null
    or t.twin_is_mane_select = 0
    or coalesce(t.twin_pair_transcript_id, '') <> r.transcript_id)
