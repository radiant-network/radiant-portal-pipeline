{#
  The compound-het summary of an occurrence matches the pairs the portal lists.

  `comphet_genes` holds one entry per gene with its number of partners, counted
  by the occurrence insert on `germline__snv__comphet`. The portal shows the
  count and lists the partners from the pair table on click, so the two must
  agree, gene by gene. `has_comphet` is true exactly when the occurrence has a
  pair.

  Occurrences imported before RAD-88 have a NULL `has_comphet` and are skipped.
  Returns the (occurrence, gene) where the count differs, the gene is missing
  on one side, or `has_comphet` disagrees with the pairs.
#}

{{ config(tags=['tenant_health']) }}

with summary as (
    select o.part, o.seq_id, o.task_id, o.locus_id, g.symbol, g.nb
    from {{ source('tenant_db', 'germline__snv__occurrence') }} o,
         unnest(o.comphet_genes) as u(g)
),
pairs as (
    select part, seq_id, task_id, locus_id, symbol, count(1) as nb
    from {{ source('tenant_db', 'germline__snv__comphet') }}
    group by part, seq_id, task_id, locus_id, symbol
),
count_mismatches as (
    select
        coalesce(s.part, p.part) as part,
        coalesce(s.seq_id, p.seq_id) as seq_id,
        coalesce(s.task_id, p.task_id) as task_id,
        coalesce(s.locus_id, p.locus_id) as locus_id,
        coalesce(s.symbol, p.symbol) as symbol,
        s.nb as summary_nb,
        p.nb as pairs_nb
    from summary s
    full outer join pairs p
        on p.part = s.part
       and p.seq_id = s.seq_id
       and p.task_id = s.task_id
       and p.locus_id = s.locus_id
       and p.symbol = s.symbol
    where s.nb is null or p.nb is null or s.nb != p.nb
),
flag_mismatches as (
    select o.part, o.seq_id, o.task_id, o.locus_id, cast(null as varchar) as symbol, null as summary_nb, null as pairs_nb
    from {{ source('tenant_db', 'germline__snv__occurrence') }} o
    left join (select distinct part, seq_id, task_id, locus_id from pairs) p
        on p.part = o.part
       and p.seq_id = o.seq_id
       and p.task_id = o.task_id
       and p.locus_id = o.locus_id
    where o.has_comphet is not null
      and o.has_comphet != (p.locus_id is not null)
)
select c.*
from count_mismatches c
-- A pair whose occurrence predates RAD-88 has no summary yet.
left join {{ source('tenant_db', 'germline__snv__occurrence') }} o
    on o.part = c.part
   and o.seq_id = c.seq_id
   and o.task_id = c.task_id
   and o.locus_id = c.locus_id
where o.has_comphet is not null
union all
select * from flag_mismatches
