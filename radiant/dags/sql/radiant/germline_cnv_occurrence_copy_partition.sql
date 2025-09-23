SELECT *
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE seq_id NOT IN %(seq_ids)s;
