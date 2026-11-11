INSERT OVERWRITE {{ mapping.starrocks_gene_reference_transcript }}
-- One row per gene: its MANE Select transcript, else its Ensembl canonical one. The flags are true, false or
-- null (no tag at all), hence the COALESCE.
WITH candidates AS (
    SELECT t.gene_id,
           t.transcript_id,
           t.version,
           COALESCE(t.is_mane_select, false) AS is_mane_select,
           ROW_NUMBER() OVER (
               PARTITION BY t.gene_id
               ORDER BY COALESCE(t.is_mane_select, false) DESC, COALESCE(t.is_canonical, false) DESC, t.transcript_id
           ) AS rank_in_gene
    FROM {{ mapping.iceberg_ensembl_transcript }} t
    WHERE t.gene_id IS NOT NULL
      AND (COALESCE(t.is_mane_select, false) OR COALESCE(t.is_canonical, false))
),
mane_select AS (
    SELECT ensembl_transcript_id, ensembl_transcript_version, refseq_transcript_id
    FROM {{ mapping.iceberg_mane }}
    WHERE mane_status = 'MANE Select'
)
SELECT c.gene_id AS ensembl_gene_id,
       c.transcript_id AS ensembl_transcript_id,
       c.version AS ensembl_transcript_version,
       c.is_mane_select,
       -- Matched on the version too: a MANE release built on another Ensembl release leaves it null.
       m.refseq_transcript_id
FROM candidates c
LEFT JOIN mane_select m
       ON m.ensembl_transcript_id = c.transcript_id
      AND m.ensembl_transcript_version = c.version
WHERE c.rank_in_gene = 1
;
