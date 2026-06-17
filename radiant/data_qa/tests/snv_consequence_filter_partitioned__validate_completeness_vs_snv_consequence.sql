{#
  Completeness (locus grain) — the reciprocal of
  snv_consequence_filter_partitioned__validate_subset_of_snv_consequence.

  `_filter_partitioned` derives from `snv__consequence` (UNNEST + GROUP BY)
  then semi-joins `germline__snv__occurrence` on (locus_id, part). So a locus
  present in `snv__consequence` AND having a germline occurrence for a given
  `part` MUST appear in `_filter_partitioned` for that `part`. A missing one
  signals a dropped locus — a failed / partial partition overwrite or a broken
  semi-join.

  Locus grain on purpose: it targets the partition / semi-join logic without
  replaying the UNNEST + GROUP BY of the derivation.
#}

select
    o.part,
    o.locus_id
from {{ source('radiant', 'germline__snv__occurrence') }} o
join {{ source('radiant', 'snv__consequence') }} c
    on c.locus_id = o.locus_id
   and array_length(c.consequences) > 0
left join {{ source('radiant', 'snv__consequence_filter_partitioned') }} fp
    on fp.locus_id = o.locus_id
   and fp.part = o.part
where fp.locus_id is null
group by o.part, o.locus_id
