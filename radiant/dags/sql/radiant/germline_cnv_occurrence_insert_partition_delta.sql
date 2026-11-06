WITH cytoband AS (SELECT o.name, o.seq_id, o.task_id, array_agg(c.cytoband) AS cytoband
                  FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
                  JOIN {{ mapping.starrocks_cytoband }} c ON c.chromosome = o.chromosome AND c.start <= o.end AND c.end >= o.start
                  WHERE o.seq_id IN %(seq_ids)s
                    AND o.tenant_code = %(tenant_code)s
                  GROUP BY o.name, o.seq_id, o.task_id),
     genes AS (SELECT o.name, o.seq_id, o.task_id, array_agg(g.name) AS symbol
               FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
               JOIN {{ mapping.starrocks_ensembl_gene }} g ON g.chromosome = o.chromosome AND g.start <= o.end
                    AND g.end >= o.start
               WHERE o.seq_id IN %(seq_ids)s
                 AND o.tenant_code = %(tenant_code)s
               GROUP BY o.name, o.seq_id, o.task_id),
     -- Position first, then overlap on (seq_id, chromosome, bin). Occurrence and variant are colocated on locus_id,
     -- so `snv_pos` is a local join. RAD-61: keyed on (seq_id, chromosome) alone, every CNV still met every SNV of its
     -- chromosome before the interval filter, and the planner, with no stats on the CNV side, could pick `snv_pos` as
     -- the hash table: a 166-sample WGS part filled the BE spill disk. Each CNV now has one row per 1 Mb bin it covers,
     -- so an SNV meets only the CNVs of its own bin, and exactly one row of each CNV covering it (nb_snv unchanged).
     -- The CNV bins are the small side: broadcast them as the hash table, never `snv_pos`.
     snv_pos AS (SELECT s.seq_id, s.locus_id, v.chromosome, v.start, v.start DIV 1000000 AS bin
                 FROM {{ mapping.starrocks_germline_snv_occurrence }} s
                 JOIN {{ mapping.starrocks_snv_variant }} v ON v.locus_id = s.locus_id
                 WHERE s.part = {{ partition }} AND s.seq_id IN %(seq_ids)s
                   -- RAD-57: same quality gate as the frequencies, so nb_snv counts quality-passing SNVs only.
                   AND s.gq >= 20 AND s.filter = 'PASS' AND s.ad_alt >= 3),
     cnv_bin AS (SELECT o.name, o.seq_id, o.task_id, o.chromosome, o.start, o.end, b.bin
                 FROM {{ mapping.iceberg_germline_cnv_occurrence }} o,
                      unnest(array_generate(o.start DIV 1000000, o.end DIV 1000000)) AS b(bin)
                 WHERE o.seq_id IN %(seq_ids)s
                   AND o.tenant_code = %(tenant_code)s),
     snv AS (SELECT c.name, c.seq_id, c.task_id, COUNT(DISTINCT p.locus_id) AS nb_snv
             FROM snv_pos p
             JOIN [BROADCAST] cnv_bin c ON c.seq_id = p.seq_id AND c.chromosome = p.chromosome AND c.bin = p.bin
                    AND p.start >= c.start AND p.start <= c.end
             GROUP BY c.name, c.seq_id, c.task_id),
    gnomad_overlaps AS (
        SELECT
            cnv.seq_id,
            cnv.task_id,
            cnv.name,
            gnomad.af as af,
            -- Only the `gnomad_sv_v1` contract table has `ac`: on the legacy table, gnomad_ac stays NULL.
            {% if mapping.iceberg_gnomad_sv_is_contract %}gnomad.ac{% else %}CAST(NULL AS INT){% endif %} as ac,
            gnomad.n_het + gnomad.n_homalt as sc,
            gnomad.n_het as sc_het,
            gnomad.n_homalt as sc_hom,
            gnomad.n_bi_genos as sn,
            (gnomad.n_het + gnomad.n_homalt) / gnomad.n_bi_genos as sf
        FROM {{ mapping.iceberg_germline_cnv_occurrence }} cnv
        JOIN {{ mapping.iceberg_gnomad_sv }} gnomad
        /* Key on `type`, not `alternate`, so this file stays a near-copy of the somatic one, where an LOH
           event stores `<LOH>` and matches no gnomAD svtype. Behaviour-preserving here: germline `type` is
           derived from `alternate` (<DUP> -> GAIN, <DEL> -> LOSS, else UNKNOWN), so the CASE reproduces
           today's matches exactly and UNKNOWN yields NULL, just as an odd ALT fails to match today. The CASE
           yields only 'DUP', 'DEL' or NULL, which is why the separate `svtype IN ('DUP','DEL')` filter goes. */
        ON cnv.chromosome = gnomad.chromosome
        AND gnomad.svtype = CASE WHEN cnv.type IN ('GAIN', 'GAINLOH') THEN 'DUP'
                                 WHEN cnv.type = 'LOSS'               THEN 'DEL' END
        WHERE
            /* Reciprocal overlap of at least 80 percent */
            GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start)) >= 0.8 * (cnv.end - cnv.start)
        AND
            GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start)) >= 0.8 * (gnomad.end - gnomad.start)
{% if not mapping.iceberg_gnomad_sv_is_contract %}
		/* `gnomad_sv_v1` Contract table filters the rows, but not the legacy table  */
        AND gnomad.filters = 'PASS'
{% endif %}
        AND cnv.seq_id IN %(seq_ids)s
        AND cnv.tenant_code = %(tenant_code)s
    ),
    gnomad_ranked AS (
        SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY o.seq_id, o.task_id, o.name
            ORDER BY o.af DESC, o.sf DESC
        ) AS rn
        FROM gnomad_overlaps o
    ),
    -- RAD-181: the CNV Exomiser report of the CNV annotation task, loaded for the proband by `load_exomiser_cnv_files`.
    -- Tagged with the annotation task id, so the DRAGEN set of an alignment task never matches. Several rows for one
    -- record (one per gene or mode of inheritance): the highest gene combined score wins, as the SNV
    -- `variant_rank = 1` rule does. The report's `VCF_ID` depends on the VCF cnv-post-processing ran Exomiser on
    -- (workflows/cnv_post_processing.nf, STAGE 8):
    --   - the proband VCF (JOIN_PROBAND_CNV, the usual route): the proband's own call ID, that is `name` (FORMAT/OID);
    --   - the family VCF (no proband VCF for the phenopacket's proband, e.g. a singleton): the slivar record's ID, that
    --     is `vcf_id` (the call truvari kept, often a parent's).
    -- So the occurrence takes the row matching its `name` (exomiser_by_name), else the row matching its `vcf_id`
    -- (exomiser_by_vcf_id). Where `name` = `vcf_id` both are the same row. Both keys are DRAGEN call IDs, which carry
    -- the call's type and interval (`DRAGEN:LOSS:chr1:10001-20000`): a wrong match needs the same call ID on two
    -- records of one task.
    exomiser_ranked AS (
        SELECT
            e.seq_id,
            e.task_id,
            e.vcf_id,
            e.rank,
            e.variant_score,
            e.gene_combined_score,
            e.moi,
            e.acmg_classification,
            e.acmg_evidence,
            ROW_NUMBER() OVER (
                PARTITION BY e.seq_id, e.task_id, e.vcf_id
                ORDER BY e.gene_combined_score DESC, e.rank ASC, e.variant_score DESC, e.moi ASC
            ) AS rn
        FROM {{ mapping.starrocks_staging_exomiser_cnv }} e
        WHERE e.part = {{ partition }}
          AND e.task_id IN %(task_ids)s
          AND e.tenant_code = %(tenant_code)s
    )
SELECT o.part,
       o.seq_id,
       o.task_id,
       GET_CNV_ID(o.chromosome, o.start, o.length, o.type) as cnv_id,
       o.aliquot,
       o.chromosome,
       o.alternate,
       o.start,
       o.end,
       o.type,
       o.length,
       o.name,
       o.quality,
       o.calls,
       o.filter,
       o.bc,
       o.cn,
       o.pe,
       o.sm,
       o.svtype,
       o.svlen,
       o.reflen,
       o.ciend,
       o.cipos,
       o.phased,
       cytoband.cytoband, genes.symbol, array_length(genes.symbol) AS nb_genes,
       snv.nb_snv,
       gnomad_ranked.af AS gnomad_af,
       gnomad_ranked.sc AS gnomad_sc,
       gnomad_ranked.sn AS gnomad_sn,
       gnomad_ranked.sf AS gnomad_sf,
       gnomad_ranked.sc_hom AS gnomad_sc_hom,
       gnomad_ranked.sc_het AS gnomad_sc_het,
       gnomad_ranked.ac AS gnomad_ac,
       -- The slivar set's columns, NULL on the DRAGEN set. Last, as in the table: the insert is positional.
       o.parental_origin,
       o.transmission_mode,
       o.mother_calls,
       o.mother_cn,
       o.mother_bc,
       o.mother_pe,
       o.mother_sm,
       o.mother_name,
       o.father_calls,
       o.father_cn,
       o.father_bc,
       o.father_pe,
       o.father_sm,
       o.father_name,
       -- The CNV Exomiser columns (RAD-181), NULL on the DRAGEN set and on a record Exomiser did not rank. Last, as in
       -- the table: the insert is positional.
       IF(exomiser_by_name.vcf_id IS NOT NULL, exomiser_by_name.rank, exomiser_by_vcf_id.rank) AS exomiser_rank,
       IF(exomiser_by_name.vcf_id IS NOT NULL, exomiser_by_name.variant_score, exomiser_by_vcf_id.variant_score)
           AS exomiser_variant_score,
       IF(exomiser_by_name.vcf_id IS NOT NULL, exomiser_by_name.gene_combined_score,
          exomiser_by_vcf_id.gene_combined_score) AS exomiser_gene_combined_score,
       IF(exomiser_by_name.vcf_id IS NOT NULL, exomiser_by_name.moi, exomiser_by_vcf_id.moi) AS exomiser_moi,
       IF(exomiser_by_name.vcf_id IS NOT NULL, exomiser_by_name.acmg_classification,
          exomiser_by_vcf_id.acmg_classification) AS exomiser_acmg_classification,
       IF(exomiser_by_name.vcf_id IS NOT NULL, exomiser_by_name.acmg_evidence, exomiser_by_vcf_id.acmg_evidence)
           AS exomiser_acmg_evidence
FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
         -- Keyed on task_id too. Both sets of one sequencing (DRAGEN and slivar) can hold the same call name.
         LEFT JOIN cytoband ON cytoband.seq_id = o.seq_id AND cytoband.task_id = o.task_id AND o.name = cytoband.name
         LEFT JOIN genes ON genes.seq_id = o.seq_id AND genes.task_id = o.task_id AND o.name = genes.name
         LEFT JOIN snv ON snv.seq_id = o.seq_id AND snv.task_id = o.task_id AND o.name = snv.name
         LEFT JOIN gnomad_ranked ON gnomad_ranked.seq_id = o.seq_id AND gnomad_ranked.task_id = o.task_id
              AND o.name = gnomad_ranked.name AND gnomad_ranked.rn = 1
         LEFT JOIN exomiser_ranked exomiser_by_name ON exomiser_by_name.seq_id = o.seq_id
              AND exomiser_by_name.task_id = o.task_id AND exomiser_by_name.vcf_id = o.name AND exomiser_by_name.rn = 1
         LEFT JOIN exomiser_ranked exomiser_by_vcf_id ON exomiser_by_vcf_id.seq_id = o.seq_id
              AND exomiser_by_vcf_id.task_id = o.task_id AND exomiser_by_vcf_id.vcf_id = o.vcf_id
              AND exomiser_by_vcf_id.rn = 1
WHERE o.seq_id IN %(seq_ids)s
  AND o.task_id IN %(task_ids)s
  AND o.tenant_code = %(tenant_code)s
;
