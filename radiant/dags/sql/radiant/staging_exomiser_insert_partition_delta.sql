INSERT INTO {{ params.starrocks_staging_exomiser }} TEMPORARY PARTITION (tp%(part)s)
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
FROM {{ params.starrocks_staging_exomiser }} PARTITION p%(part)s
WHERE seq_id NOT IN %(seq_ids)s;