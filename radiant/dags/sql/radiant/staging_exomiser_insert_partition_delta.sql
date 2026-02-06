SELECT part,
       seq_id,
       id,
       rank,
       symbol,
       entrez_gene_id,
       moi,
       variant_score,
       gene_combined_score,
       contributing_variant,
       chromosome,
       start,
       end,
       reference,
       alternate,
       acmg_classification,
       acmg_evidence
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE seq_id NOT IN %(seq_ids)s AND seq_id NOT IN %(deleted_seq_ids)s;