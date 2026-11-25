-- The germline CNV VCF of each experiment's current germline alignment, and how many there are: one, or a mistyped
-- document. Counted over that one task, as `current_germline_gvcf`. Selected on the document's type fields, never its
-- file name: `gcnv` is germline (`scnv` is somatic).
CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_current_germline_gcnv }} AS
SELECT ca.seq_id             AS seq_id,
       ca.task_id            AS task_id,
       MAX(d.url)            AS url,
       COUNT(DISTINCT d.url) AS matches
FROM {{ mapping.starrocks_current_alignment }} ca
JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id
                                                 AND thd.type = 'output'
JOIN {{ mapping.clinical_document }} d            ON d.id = thd.document_id
                                                 AND d.data_type_code = 'gcnv'
                                                 AND d.format_code    = 'vcf'
WHERE ca.task_type = 'alignment_germline_variant_calling'
GROUP BY ca.seq_id, ca.task_id
