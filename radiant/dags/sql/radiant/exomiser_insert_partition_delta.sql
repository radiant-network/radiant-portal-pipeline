select e.part,
       seq_id,
       task_id,
       locus_id,
       id,
       e.locus_hash,
       moi,
       variant_score,
       gene_combined_score,
       ROW_NUMBER() OVER (PARTITION BY e.locus_hash, seq_id, task_id ORDER BY gene_combined_score DESC) AS variant_rank,
       rank,
       e.symbol,
       acmg_classification,
       acmg_evidence
FROM {{ mapping.starrocks_staging_exomiser }} e
JOIN {{ mapping.starrocks_snv_tmp_variant }} v ON e.locus_hash = v.locus_hash
WHERE part = {{ partition }} AND task_id IN %(task_ids)s AND task_id NOT IN %(deleted_task_ids)s
AND e.tenant_code = %(tenant_code)s;
