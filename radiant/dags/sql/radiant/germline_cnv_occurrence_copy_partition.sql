SELECT *
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE seq_id NOT IN %(seq_ids)s and seq_id NOT IN %(deleted_seq_ids)s;
