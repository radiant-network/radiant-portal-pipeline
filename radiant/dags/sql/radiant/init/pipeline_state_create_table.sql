-- The last row of each pipeline item in `pipeline_journal`: where each item stands, and since when.
CREATE VIEW IF NOT EXISTS {{ mapping.starrocks_pipeline_state }} AS
SELECT case_id, pipeline, item_key, decided_at, run_id, decision, reasons
FROM (
    SELECT j.*,
           ROW_NUMBER() OVER (PARTITION BY case_id, pipeline, item_key ORDER BY decided_at DESC) AS rn
    FROM {{ mapping.starrocks_pipeline_journal }} j
) ranked
WHERE rn = 1
