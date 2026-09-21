-- Re-annotation counterpart of `germline_cnv_occurrence_insert_partition_delta.sql` (SJRA-1811 §5, P3b).
--
-- Differences from the ingest statement:
--   1. Every reference to the Iceberg occurrence table is replaced by the StarRocks one. That is the
--      Iceberg-retention dependency this ticket exists to remove -- §5, "CNV occurrences | the StarRocks
--      occurrence tables".
--   2. Scoped by `part`, not by `seq_ids`: a re-annotation rebuilds whole partitions, not a delta.
--   3. No `tenant_code` predicate. The Iceberg table is shared across tenants; the StarRocks one lives in
--      the tenant database, so the tenant is already implied by `mapping`.
--   4. `cnv_id` is read back rather than recomputed. It is derived from columns this statement carries
--      through untouched, so GET_CNV_ID would return the value already stored.
--
INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_germline_cnv_occurrence }}
WITH cytoband AS (SELECT o.name, o.seq_id, array_agg(c.cytoband) AS cytoband
                  FROM {{ mapping.starrocks_germline_cnv_occurrence }} o
                  JOIN {{ mapping.starrocks_cytoband }} c ON c.chromosome = o.chromosome AND c.start <= o.end AND c.end >= o.start
                  WHERE o.part = %(part)s
                  GROUP BY o.name, o.seq_id),
     genes AS (SELECT o.name, o.seq_id, array_agg(g.name) AS symbol
               FROM {{ mapping.starrocks_germline_cnv_occurrence }} o
               JOIN {{ mapping.starrocks_ensembl_gene }} g ON g.chromosome = o.chromosome AND g.start <= o.end
                    AND g.end >= o.start
               WHERE o.part = %(part)s
               GROUP BY o.name, o.seq_id),
     snv AS (SELECT o.name, o.seq_id, COUNT(DISTINCT s.locus_id) AS nb_snv
             FROM {{ mapping.starrocks_germline_cnv_occurrence }} o
             JOIN {{ mapping.starrocks_germline_snv_occurrence }} s ON s.seq_id = o.seq_id
                    AND s.part = %(part)s
             JOIN {{ mapping.starrocks_snv_variant }} v ON v.locus_id = s.locus_id
                    AND v.chromosome = o.chromosome AND v.start <= o.end AND v.start >= o.start
             WHERE o.part = %(part)s
             GROUP BY o.name, o.seq_id),
    gnomad_overlaps AS (
        SELECT
            cnv.seq_id,
            cnv.name,
            gnomad.af as af,
            gnomad.n_het + gnomad.n_homalt as sc,
            gnomad.n_het as sc_het,
            gnomad.n_homalt as sc_hom,
            gnomad.n_bi_genos as sn,
            (gnomad.n_het + gnomad.n_homalt) / gnomad.n_bi_genos as sf
        FROM {{ mapping.starrocks_germline_cnv_occurrence }} cnv
        JOIN {{ mapping.iceberg_gnomad_sv }} gnomad
        /* Key on `type`, not `alternate`, so this file stays a near-copy of the somatic one, where an LOH
           event stores `<LOH>` and matches no gnomAD svtype. The CASE yields only 'DUP', 'DEL' or NULL,
           which is why no separate `svtype IN ('DUP','DEL')` filter is needed. */
        ON cnv.chromosome = gnomad.chromosome
        AND gnomad.svtype = CASE WHEN cnv.type IN ('GAIN', 'GAINLOH') THEN 'DUP'
                                 WHEN cnv.type = 'LOSS'               THEN 'DEL' END
        WHERE
            /* Reciprocal overlap of at least 80 percent */
            GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start)) >= 0.8 * (cnv.end - cnv.start)
        AND
            GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start)) >= 0.8 * (gnomad.end - gnomad.start)
{% if not mapping.iceberg_gnomad_sv_is_contract %}
        AND gnomad.filters = 'PASS'
{% endif %}
        AND cnv.part = %(part)s
    ),
    gnomad_ranked AS (
        SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY o.seq_id, o.name
            ORDER BY o.af DESC, o.sf DESC
        ) AS rn
        FROM gnomad_overlaps o
    )
SELECT o.part,
       o.seq_id,
       o.task_id,
       o.cnv_id,
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
       gnomad_ranked.sc_het AS gnomad_sc_het
FROM {{ mapping.starrocks_germline_cnv_occurrence }} o
         LEFT JOIN cytoband ON cytoband.seq_id = o.seq_id AND o.name = cytoband.name
         LEFT JOIN genes ON genes.seq_id = o.seq_id AND o.name = genes.name
         LEFT JOIN snv ON snv.seq_id = o.seq_id AND o.name = snv.name
         LEFT JOIN gnomad_ranked ON gnomad_ranked.seq_id = o.seq_id AND o.name = gnomad_ranked.name AND gnomad_ranked.rn = 1
WHERE o.part = %(part)s
;
