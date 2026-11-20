-- RAD-315. The `covgene` file of the current alignment of each experiment of the part's delta, one row per alignment
-- task. Read from the experiment, not from the staging rows: the proband of a case with a slivar VCF and the normal of
-- a tumor-normal case keep their coverage. `tenant_code` tags the rows of the shared staging table.
SELECT ca.seq_id, ca.task_id, se.tenant_code, d.url AS filepath
FROM {{ mapping.starrocks_current_alignment }} ca
JOIN {{ mapping.clinical_sequencing_experiment }} se ON se.id = ca.seq_id
JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id AND thd.type = 'output'
JOIN {{ mapping.clinical_document }} d ON d.id = thd.document_id
WHERE d.data_type_code = 'covgene'
  AND d.format_code = 'csv'
  AND (ca.seq_id IN %(seq_ids)s OR ca.seq_id IN %(deleted_seq_ids)s)
-- A task with more than one `covgene` file: the newest one, or every gene would load twice.
QUALIFY ROW_NUMBER() OVER (PARTITION BY ca.task_id ORDER BY d.id DESC) = 1
ORDER BY seq_id, task_id
