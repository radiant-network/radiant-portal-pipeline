SELECT *
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE seq_id IN %(seq_ids)s;