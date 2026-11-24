-- RAD-315. Keyed on the experiment, as the staging copy: a re-alignment replaces the coverage of the old alignment.
SELECT *
FROM {{ table }} PARTITION ( p{{ partition }})
WHERE seq_id NOT IN %(seq_ids)s AND seq_id NOT IN %(deleted_seq_ids)s;
