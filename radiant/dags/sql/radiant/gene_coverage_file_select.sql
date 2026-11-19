-- RAD-315. The `covgene` file of the current alignment of each experiment of the part's delta: one row per alignment
-- task, whatever the number of cases that share it. The current alignment is read from the experiment, not from the
-- staging rows: the proband of a case with a slivar VCF and the normal of a tumor-normal case keep their coverage, even
-- though the staging view leaves their alignment task out. `tenant_code` tags the rows of the shared staging table.
SELECT seq_id, task_id, task_type, tenant_code, filepath
FROM (
    SELECT ca.seq_id,
           ca.task_id,
           ca.task_type,
           se.tenant_code,
           d.url AS filepath,
           -- A task with more than one `covgene` document: the newest one.
           ROW_NUMBER() OVER (PARTITION BY ca.task_id, ca.seq_id ORDER BY d.id DESC) AS rn
    FROM {{ mapping.starrocks_current_alignment }} ca
    JOIN {{ mapping.clinical_sequencing_experiment }} se ON se.id = ca.seq_id
    JOIN {{ mapping.clinical_task_has_document }} thd ON thd.task_id = ca.task_id AND thd.type = 'output'
    JOIN {{ mapping.clinical_document }} d ON d.id = thd.document_id
    WHERE d.data_type_code = 'covgene'
      AND (ca.seq_id IN %(seq_ids)s OR ca.seq_id IN %(deleted_seq_ids)s)
) covgene
WHERE rn = 1
ORDER BY seq_id, task_id
