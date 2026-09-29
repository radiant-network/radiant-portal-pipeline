-- P4: promote what the import loaded to what the portal-facing tables are annotated against.
--
-- A copy, not a fresh `$refs` read. What the rebuilds just annotated is whatever P1 loaded into
-- StarRocks; re-resolving the ref here would stamp a publish that landed *during* the rebuilds as
-- annotated, and the next run's gate would then skip the rebuild that publish actually needs.
--
-- Every source, including the ones whose branch was gated out -- a branch is skipped precisely
-- because none of its sources moved, so for those two columns were already equal.
--
-- UPDATE rather than an upsert because this writes two columns of a row the import owns. StarRocks
-- supports UPDATE on Primary Key tables (v2.3+), and `open_data_release` is PRIMARY KEY(source_name).
-- The WHERE clause is mandatory there, so it names the non-nullable primary key to mean every row.
UPDATE {{ mapping.starrocks_open_data_release }}
SET reannotated_snapshot_id = imported_snapshot_id,
    recorded_at = NOW(),
    dag_run_id  = '{{ run_id }}'
WHERE source_name IS NOT NULL;
