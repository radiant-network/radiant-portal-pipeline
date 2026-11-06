{#
  RAD-181: the CNV Exomiser columns of germline__cnv__occurrence are a copy of the best row (`variant_rank = 1`) of
  `exomiser_cnv` for the same occurrence (part, seq_id, task_id, name). Both tables are rebuilt in the same
  `import_part` run, and their copy steps keep the same tasks, so the two must agree:

    - an occurrence with an `exomiser_rank` has exactly one `variant_rank = 1` row, with the same values;
    - a `variant_rank = 1` row has its occurrence, and that occurrence carries its values;
    - an occurrence has at most one `variant_rank = 1` row.

  Holds for any number of cases, also none (both tables empty), so it runs on every tenant (tenant_health). It
  replaces the should_not_contain_same_value sweep on these columns, which depends on how many cases a tenant has.
  `exomiser_acmg_evidence` (an array) is not compared.
#}

{{ config(tags=['tenant_health']) }}

with best as (
    select part,
           seq_id,
           task_id,
           name,
           count(1)                 as n_best,
           min(rank)                as rank,
           min(variant_score)       as variant_score,
           min(gene_combined_score) as gene_combined_score,
           min(moi)                 as moi,
           min(acmg_classification) as acmg_classification
    from {{ source('tenant_db', 'exomiser_cnv') }}
    where variant_rank = 1
    group by part, seq_id, task_id, name
),

ranked as (
    select part,
           seq_id,
           task_id,
           name,
           exomiser_rank,
           exomiser_variant_score,
           exomiser_gene_combined_score,
           exomiser_moi,
           exomiser_acmg_classification
    from {{ source('tenant_db', 'germline__cnv__occurrence') }}
    where exomiser_rank is not null
)

select coalesce(r.part, b.part)       as part,
       coalesce(r.seq_id, b.seq_id)   as seq_id,
       coalesce(r.task_id, b.task_id) as task_id,
       coalesce(r.name, b.name)       as name,
       case
           when b.task_id is null then 'ranked occurrence without a best exomiser_cnv row'
           when r.task_id is null then 'best exomiser_cnv row not copied onto its occurrence'
           when b.n_best <> 1 then 'several best exomiser_cnv rows'
           else 'values differ from the best exomiser_cnv row'
       end                            as reason
from ranked r
full outer join best b
    on  b.part = r.part
    and b.seq_id = r.seq_id
    and b.task_id = r.task_id
    and b.name = r.name
where b.task_id is null
   or r.task_id is null
   or b.n_best <> 1
   or not (r.exomiser_rank <=> b.rank
           and r.exomiser_variant_score <=> b.variant_score
           and r.exomiser_gene_combined_score <=> b.gene_combined_score
           and r.exomiser_moi <=> b.moi
           and r.exomiser_acmg_classification <=> b.acmg_classification)
