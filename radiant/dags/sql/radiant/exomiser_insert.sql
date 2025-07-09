INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ params.starrocks_exomiser }}
select e.part,
       seq_id,
       locus_id,
       id,
       e.locus_hash,
       moi,
       variant_score,
       gene_combined_score,
       ROW_NUMBER() OVER (PARTITION BY e.locus_hash, seq_id ORDER BY gene_combined_score DESC) AS variant_rank,
       rank,
       e.symbol,
       acmg_classification,
       acmg_evidence
FROM {{ params.starrocks_staging_exomiser }} e
JOIN {{ params.starrocks_tmp_variant }} v ON e.locus_hash = v.locus_hash
WHERE part = %(part)s;