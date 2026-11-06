-- RAD-181: the CNV Exomiser rows of one tenant, attached to their germline CNV occurrence (the twin of
-- exomiser_insert_partition_delta.sql). Reads the rows `load_exomiser_cnv_files` broker-loaded into
-- `raw_exomiser_cnv`, tagged with the CNV annotation task, so the DRAGEN set of an alignment task never matches.
--
-- The report's `VCF_ID` depends on the VCF cnv-post-processing ran Exomiser on (workflows/cnv_post_processing.nf,
-- STAGE 8):
--   - the proband VCF (JOIN_PROBAND_CNV, the usual route): the proband's own call ID, that is `name` (FORMAT/OID);
--   - the family VCF (no proband VCF for the phenopacket's proband): the slivar record's ID, that is the Iceberg-only
--     `vcf_id` (the call truvari kept, often a parent's).
-- So an occurrence takes the rows matching its `name`, else the rows matching its `vcf_id`. Where `name` = `vcf_id`
-- both are the same rows. Both keys are DRAGEN call IDs, which carry the call's type and interval
-- (`DRAGEN:LOSS:chr1:10001-20000`): a wrong match needs the same call ID on two records of one task.
--
-- A report row that matches no occurrence of the task is dropped, as the SNV insert drops a locus it cannot place.
-- `variant_rank`: the highest gene combined score first, as for SNVs; the rank, the variant score and the MOI only
-- break ties, so the row the occurrence copies is deterministic.
WITH occurrence AS (
    SELECT o.seq_id,
           o.task_id,
           o.name,
           o.vcf_id,
           GET_CNV_ID(o.chromosome, o.start, o.length, o.type) AS cnv_id
    FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
    WHERE o.seq_id IN %(seq_ids)s
      AND o.task_id IN %(task_ids)s
      AND o.tenant_code = %(tenant_code)s
),
report AS (
    SELECT e.part, e.seq_id, e.task_id, e.vcf_id, e.id, e.moi, e.variant_score, e.gene_combined_score, e.rank,
           e.symbol, e.acmg_classification, e.acmg_evidence
    FROM {{ mapping.starrocks_staging_exomiser_cnv }} e
    WHERE e.part = {{ partition }}
      AND e.task_id IN %(task_ids)s
      AND e.task_id NOT IN %(deleted_task_ids)s
      AND e.tenant_code = %(tenant_code)s
),
-- One row per (occurrence, report row) and per key that matched: 1 for `name`, 2 for `vcf_id`. The `vcf_id` branch
-- skips an occurrence whose `vcf_id` is its `name`: the first branch has its rows already.
matched AS (
    SELECT r.part, o.seq_id, o.task_id, o.cnv_id, r.id, o.name, r.vcf_id, r.moi, r.variant_score,
           r.gene_combined_score, r.rank, r.symbol, r.acmg_classification, r.acmg_evidence, 1 AS key_order
    FROM occurrence o
    JOIN report r ON r.seq_id = o.seq_id AND r.task_id = o.task_id AND r.vcf_id = o.name
    UNION ALL
    SELECT r.part, o.seq_id, o.task_id, o.cnv_id, r.id, o.name, r.vcf_id, r.moi, r.variant_score,
           r.gene_combined_score, r.rank, r.symbol, r.acmg_classification, r.acmg_evidence, 2 AS key_order
    FROM occurrence o
    JOIN report r ON r.seq_id = o.seq_id AND r.task_id = o.task_id AND r.vcf_id = o.vcf_id
    WHERE o.vcf_id <> o.name
),
-- The `name` rows of an occurrence when it has some, else its `vcf_id` rows.
kept AS (
    SELECT *
    FROM (
        SELECT m.*, MIN(m.key_order) OVER (PARTITION BY m.seq_id, m.task_id, m.name) AS min_key_order
        FROM matched m
    ) k
    WHERE k.key_order = k.min_key_order
)
SELECT part,
       seq_id,
       task_id,
       cnv_id,
       id,
       name,
       vcf_id,
       moi,
       variant_score,
       gene_combined_score,
       ROW_NUMBER() OVER (
           PARTITION BY seq_id, task_id, name
           ORDER BY gene_combined_score DESC, rank ASC, variant_score DESC, moi ASC
       ) AS variant_rank,
       rank,
       symbol,
       acmg_classification,
       acmg_evidence
FROM kept;
