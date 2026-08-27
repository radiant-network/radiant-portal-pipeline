{#
  A given (locus_id, sequencing) call must not be reported as BOTH a germline
  occurrence and a somatic TUMOR occurrence: a tumor sequencing analyzed as
  germline (or vice-versa) is contradictory.
#}

select
    g.locus_id,
    g.seq_id
from {{ source('tenant_db', 'germline__snv__occurrence') }} g
join {{ source('tenant_db', 'somatic__snv__occurrence') }} s
    on s.locus_id = g.locus_id
   and s.tumor_seq_id = g.seq_id
