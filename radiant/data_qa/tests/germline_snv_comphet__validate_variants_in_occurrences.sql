{#
  Both variants of a compound-het pair must be occurrences of the same sample
  and task. The pairs and the occurrences are loaded from the same VCF and both
  hashes resolve through `snv__tmp_variant`, so a pair whose variant or partner
  has no occurrence signals a locus key mismatch (e.g. a slivar partner position
  not hashed like the VCF record) or a pair table out of sync with the
  occurrences (one partition swap failed).

  Task 2 of RAD-39 counts these pairs on the occurrence: a missing partner would
  be counted but could not be listed.
#}

{{ config(tags=['tenant_health']) }}

select
    c.part,
    c.seq_id,
    c.task_id,
    c.locus_id,
    c.partner_locus_id
from {{ source('tenant_db', 'germline__snv__comphet') }} c
left join {{ source('tenant_db', 'germline__snv__occurrence') }} o
    on o.part = c.part
   and o.seq_id = c.seq_id
   and o.task_id = c.task_id
   and o.locus_id = c.locus_id
left join {{ source('tenant_db', 'germline__snv__occurrence') }} p
    on p.part = c.part
   and p.seq_id = c.seq_id
   and p.task_id = c.task_id
   and p.locus_id = c.partner_locus_id
where o.locus_id is null or p.locus_id is null
