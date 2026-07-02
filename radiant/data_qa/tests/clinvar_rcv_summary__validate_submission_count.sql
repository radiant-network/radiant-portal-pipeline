{#
  submission_count is a count of ClinVar submissions and must be non-negative.
#}

select
    locus_id,
    clinvar_id,
    submission_count
from {{ source('radiant', 'clinvar_rcv_summary') }}
where submission_count < 0