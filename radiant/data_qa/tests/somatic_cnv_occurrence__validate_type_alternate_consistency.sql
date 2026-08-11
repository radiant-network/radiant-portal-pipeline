{#
  `alternate` is derived from the resolved `type`, never copied from the VCF:
    - GAIN -> <DUP>, LOSS -> <DEL>, CNLOH / GAINLOH -> <LOH>.
#}

select
    cnv_id,
    chromosome,
    start,
    `end`,
    type,
    alternate
from {{ source('radiant', 'somatic__cnv__occurrence') }}
where (type = 'GAIN' and alternate != '<DUP>')
   or (type = 'LOSS' and alternate != '<DEL>')
   or (type in ('CNLOH', 'GAINLOH') and alternate != '<LOH>')
