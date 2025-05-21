INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ params.starrocks_staging_variants_frequencies }}
WITH patients_total_count AS (
    SELECT
        COUNT(DISTINCT s.patient_id) AS cnt
    FROM {{ params.starrocks_sequencing_experiments }} s where s.seq_id in (select seq_id from {{ params.starrocks_occurrences }} where part=%(part)s)
),
freqs as (
    SELECT o.part,
        o.locus_id,
        COUNT(DISTINCT patient_id) AS pc,
        (SELECT cnt FROM patients_total_count) AS pn
    FROM {{ params.starrocks_occurrences }} o
    JOIN {{ params.starrocks_sequencing_experiments }} s ON s.seq_id = o.seq_id
    WHERE o.part = %(part)s
    GROUP BY locus_id, o.part
)
SELECT part, locus_id, pc, pn from freqs;