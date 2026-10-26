{#
  hpo_term_id must be a well-formed HPO id (HP:NNNNNNN). Catches CSV header rows or
  malformed values leaking into the panel during ingestion (e.g. the literal 'hpo_id'
  header row observed with symbol='gene_symbol', panel='hpo_name(hpo_id)').
#}

select
    symbol,
    panel,
    hpo_term_id
from {{ source('radiant', 'hpo_gene_panel') }}
where hpo_term_id is not null
  and hpo_term_id not regexp '^HP:[0-9]{7}$'
