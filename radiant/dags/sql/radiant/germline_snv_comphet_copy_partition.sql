SELECT *
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE task_id NOT IN %(task_ids)s AND task_id NOT IN %(deleted_task_ids)s;
