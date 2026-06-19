{#
  `snv__consequence_filter_partitioned` is derived from `snv__consequence`
  via UNNEST(consequences) + GROUP BY (locus_id, symbol, …), then filtered by
  a semi-join to occurrences. Every row must therefore trace back to a base
  `snv__consequence` row with the same (locus_id, symbol) whose `consequences`
  array contains the unnested scalar `consequence`. A row that doesn't is an
  orphan — a derivation bug or a stale partition overwrite. Matched on
  categorical columns only (no fragile float equality).
#}

select
    fp.part,
    fp.locus_id,
    fp.symbol,
    fp.consequence
from {{ source('radiant', 'snv__consequence_filter_partitioned') }} fp
left join {{ source('radiant', 'snv__consequence') }} c
    on c.locus_id = fp.locus_id
   and c.symbol = fp.symbol
   and array_contains(c.consequences, fp.consequence)
where c.locus_id is null
