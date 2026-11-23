-- RAD-315. The rows to keep: every experiment the run does not reload. The rows of the delta's experiments go, keyed
-- on the experiment and not on the task, so that a re-alignment replaces the coverage of the old alignment.
SELECT part,
       seq_id,
       task_id,
       tenant_code,
       gene,
       type,
       size,
       average_coverage,
       coverage_5x,
       coverage_15x,
       coverage_30x,
       coverage_50x,
       coverage_100x,
       coverage_200x,
       coverage_300x,
       coverage_400x,
       coverage_500x,
       coverage_1000x
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE seq_id NOT IN %(seq_ids)s AND seq_id NOT IN %(deleted_seq_ids)s;
