-- The task that has each document as its output. The portal lets a document be the output of one task only, so a
-- pipeline run finds the task it registered from the URL of one of its outputs.
SELECT d.url, thd.task_id
FROM {{ mapping.clinical_document }} d
JOIN {{ mapping.clinical_task_has_document }} thd ON thd.document_id = d.id AND thd.type = 'output'
WHERE d.url IN %(urls)s
