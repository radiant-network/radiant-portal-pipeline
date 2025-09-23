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
             GROUP BY o.name, o.seq_id)
SELECT o.*, cytoband.cytoband, genes.symbol, array_length(genes.symbol) AS nb_genes, nb_snv
FROM {{ mapping.iceberg_germline_cnv_occurrence }} o
         LEFT JOIN cytoband ON cytoband.seq_id = o.seq_id AND o.name = cytoband.name
         LEFT JOIN genes ON genes.seq_id = o.seq_id AND o.name = genes.name
         LEFT JOIN snv ON snv.seq_id = o.seq_id AND o.name = snv.name
WHERE o.seq_id IN %(seq_ids)s;