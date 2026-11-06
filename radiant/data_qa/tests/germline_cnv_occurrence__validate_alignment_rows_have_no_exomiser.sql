{#
  RAD-181: the CNV Exomiser report is loaded for the CNV annotation task (the proband's slivar set) only. The
  occurrence insert joins it on the task id, so a row of an `alignment_germline_variant_calling` task (the DRAGEN set)
  must keep the six Exomiser columns NULL. A filled one means a CNV report reached the wrong set.

  Gated on staging like germline_cnv_occurrence__validate_annotation_rows_are_proband.sql;
  staging_sequencing_experiment lives in the base database (SR_SCHEMA).
#}

select
    o.task_id,
    o.seq_id,
    count(1) as occurrences
from {{ source('tenant_db', 'germline__cnv__occurrence') }} o
join {{ adapter.quote(env_var('SR_SCHEMA', 'radiant')) }}.staging_sequencing_experiment s
    on s.task_id = o.task_id
   and s.seq_id = o.seq_id
where s.task_type = 'alignment_germline_variant_calling'
  and (o.exomiser_rank is not null
       or o.exomiser_variant_score is not null
       or o.exomiser_gene_combined_score is not null
       or o.exomiser_moi is not null
       or o.exomiser_acmg_classification is not null
       or o.exomiser_acmg_evidence is not null)
group by o.task_id, o.seq_id
