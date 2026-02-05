SELECT *
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE task_id NOT IN %(task_ids)s;
