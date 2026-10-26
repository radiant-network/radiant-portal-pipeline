-- UTC, not NOW(): `updated_at` is the portal's `updated_on`, in UTC, and the pending predicate
-- (`updated_at >= ingested_at`) compares the two. NOW() is in the StarRocks session time zone.
UPDATE {{ mapping.starrocks_staging_sequencing_experiment }}
SET ingested_at = UTC_TIMESTAMP()
WHERE task_id in %(task_ids)s
