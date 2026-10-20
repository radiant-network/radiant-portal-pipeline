SELECT c.part,
       c.seq_id,
       c.task_id,
       v.locus_id,
       p.locus_id AS partner_locus_id,
       c.symbol
FROM {{ mapping.iceberg_germline_snv_comphet }} c
JOIN {{ mapping.starrocks_snv_tmp_variant }} v ON c.locus_hash = v.locus_hash
JOIN {{ mapping.starrocks_snv_tmp_variant }} p ON c.partner_locus_hash = p.locus_hash
WHERE c.part = {{ partition }} AND c.task_id IN %(task_ids)s
AND c.tenant_code = %(tenant_code)s;
