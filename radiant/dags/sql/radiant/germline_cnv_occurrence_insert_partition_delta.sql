WITH cytoband AS (SELECT o.name, o.seq_id, array_agg(c.cytoband) AS cytoband
                  FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
                  JOIN {{ mapping.starrocks_cytoband }} c ON c.chromosome = o.chromosome AND c.start <= o.end AND c.end >= o.start
                  WHERE o.seq_id IN %(seq_ids)s
                  GROUP BY o.name, o.seq_id),
     genes AS (SELECT o.name, o.seq_id, array_agg(g.name) AS symbol
               FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
               JOIN {{ mapping.starrocks_ensembl_gene }} g ON g.chromosome = o.chromosome AND g.start <= o.end
                    AND g.end >= o.start
               WHERE o.seq_id IN %(seq_ids)s
               GROUP BY o.name, o.seq_id),
     snv AS (SELECT o.name, o.seq_id, COUNT(1) AS nb_snv
             FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
             JOIN {{ mapping.iceberg_occurrence }} s ON s.chromosome = o.chromosome AND s.start <= o.end
                    AND s.start >= o.start AND o.seq_id = s.seq_id
             WHERE s.has_alt = true AND s.seq_id IN %(seq_ids)s AND o.seq_id IN %(seq_ids)s AND s.part={{ partition }}
             GROUP BY o.name, o.seq_id),
    gnomad_overlaps AS (
        SELECT
            cnv.seq_id,
            cnv.name,
            gnomad.sf,
            gnomad.sc,
            gnomad.sn,
            GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start) + 1) AS overlap
        FROM {{ mapping.iceberg_germline_cnv_occurrence }} cnv
        JOIN {{ mapping.iceberg_gnomad_cnv }} gnomad
        ON cnv.chromosome = gnomad.chromosome AND cnv.alternate = gnomad.alternate
        WHERE GREATEST(0, LEAST(cnv.end, gnomad.end) - GREATEST(cnv.start, gnomad.start) + 1)
        >= 0.8 * (cnv.end - cnv.start + 1) AND cnv.seq_id IN %(seq_ids)s
    ),
    gnomad_ranked AS (
        SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY o.seq_id, o.name
            ORDER BY o.overlap DESC, o.sf DESC
        ) AS rn
        FROM gnomad_overlaps o
    )
SELECT o.*,
       cytoband.cytoband, genes.symbol, array_length(genes.symbol) AS nb_genes, nb_snv,
       gnomad_ranked.sc AS gnomad_sc, gnomad_ranked.sn AS gnomad_sn, gnomad_ranked.sf AS gnomad_sf
FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
         LEFT JOIN cytoband ON cytoband.seq_id = o.seq_id AND o.name = cytoband.name
         LEFT JOIN genes ON genes.seq_id = o.seq_id AND o.name = genes.name
         LEFT JOIN snv ON snv.seq_id = o.seq_id AND o.name = snv.name
         LEFT JOIN gnomad_ranked ON gnomad_ranked.seq_id = o.seq_id AND o.name = gnomad_ranked.name AND gnomad_ranked.rn = 1
WHERE o.seq_id IN %(seq_ids)s;