WITH cytoband AS (SELECT o.name, o.seq_id, array_agg(c.cytoband) AS cytoband
                  FROM {{ mapping.iceberg_somatic_cnv_occurrence }} o
                  JOIN {{ mapping.starrocks_cytoband }} c ON c.chromosome = o.chromosome AND c.start <= o.end AND c.end >= o.start
                  WHERE o.seq_id IN %(seq_ids)s
                    AND o.tenant_code = %(tenant_code)s
                  GROUP BY o.name, o.seq_id),
     genes AS (SELECT o.name, o.seq_id, array_agg(g.name) AS symbol
               FROM {{ mapping.iceberg_somatic_cnv_occurrence }} o
               JOIN {{ mapping.starrocks_ensembl_gene }} g ON g.chromosome = o.chromosome AND g.start <= o.end
                    AND g.end >= o.start
               WHERE o.seq_id IN %(seq_ids)s
                 AND o.tenant_code = %(tenant_code)s
               GROUP BY o.name, o.seq_id),
     -- Somatic segments are counted against somatic SNVs: germline SNVs inside a tumour segment would be a
     -- meaningless number. The somatic occurrence table spells its sample columns `tumor_*`, hence the
     -- `tumor_seq_id` / `tumor_has_alt` names here rather than germline's `seq_id` / `has_alt`.
     snv AS (SELECT o.name, o.seq_id, COUNT(1) AS nb_snv
             FROM {{ mapping.iceberg_somatic_cnv_occurrence }} o
             JOIN {{ mapping.iceberg_somatic_snv_occurrence }} s ON s.chromosome = o.chromosome AND s.start <= o.end
                    AND s.start >= o.start AND o.seq_id = s.tumor_seq_id
             WHERE COALESCE(s.tumor_has_alt, FALSE) AND s.tumor_seq_id IN %(seq_ids)s AND o.seq_id IN %(seq_ids)s
               AND s.part={{ partition }}
               AND o.tenant_code = %(tenant_code)s -- CNV
               AND s.tenant_code = %(tenant_code)s -- SNV
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
        FROM {{ mapping.iceberg_somatic_cnv_occurrence }} cnv
        JOIN {{ mapping.iceberg_gnomad_sv }} gnomad
        /* Key on `type`, not `alternate`: an LOH event stores `<LOH>`, which matches no gnomAD svtype.
           The CASE says what the join actually means -- same copy-number direction -- so GAINLOH matches
           DUP and CNLOH (copy-neutral) matches nothing, keeping its gnomad_* columns NULL. It also yields
           only 'DUP', 'DEL' or NULL, which is why no separate `svtype IN ('DUP','DEL')` filter is needed. */
        ON cnv.chromosome = gnomad.chromosome
        AND gnomad.svtype = CASE WHEN cnv.type IN ('GAIN', 'GAINLOH') THEN 'DUP'
                                 WHEN cnv.type = 'LOSS'               THEN 'DEL' END
        WHERE
            /* Reciprocal overlap of at least 80 percent */
            GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start)) >= 0.8 * (cnv.end - cnv.start)
        AND
            GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start)) >= 0.8 * (gnomad.end - gnomad.start)
        AND gnomad.filters = 'PASS'
        AND cnv.seq_id IN %(seq_ids)s
        AND cnv.tenant_code = %(tenant_code)s
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
-- Column order follows somatic_cnv_occurrence_create_table.sql, which differs from germline's: `cn` sits in
-- the ASCN block after `phased` rather than between `bc` and `pe`. This is a positional insert.
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
       o.pe,
       o.sm,
       o.svtype,
       o.svlen,
       o.reflen,
       o.ciend,
       o.cipos,
       o.phased,
       o.cn,
       o.cnf,
       o.cnq,
       o.mcn,
       o.mcnf,
       o.mcnq,
       o.maf,
       o.sd,
       o.ascn_as,
       cytoband.cytoband, genes.symbol, array_length(genes.symbol) AS nb_genes, nb_snv,
       gnomad_ranked.af AS gnomad_af,
       gnomad_ranked.sc AS gnomad_sc,
       gnomad_ranked.sn AS gnomad_sn,
       gnomad_ranked.sf AS gnomad_sf,
       gnomad_ranked.sc_hom AS gnomad_sc_hom,
       gnomad_ranked.sc_het AS gnomad_sc_het
FROM {{ mapping.iceberg_somatic_cnv_occurrence }} o
         LEFT JOIN cytoband ON cytoband.seq_id = o.seq_id AND o.name = cytoband.name
         LEFT JOIN genes ON genes.seq_id = o.seq_id AND o.name = genes.name
         LEFT JOIN snv ON snv.seq_id = o.seq_id AND o.name = snv.name
         LEFT JOIN gnomad_ranked ON gnomad_ranked.seq_id = o.seq_id AND o.name = gnomad_ranked.name AND gnomad_ranked.rn = 1
WHERE o.seq_id IN %(seq_ids)s
  AND o.tenant_code = %(tenant_code)s
;
