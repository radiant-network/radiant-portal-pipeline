-- Re-annotation counterpart of `somatic_cnv_occurrence_insert_partition_delta.sql` (SJRA-1811 §5, P3b).
-- Same four differences from the ingest statement as the germline file -- see
-- `germline_cnv_occurrence_reannotate_partition.sql` for the reasoning.
INSERT /*+set_var(dynamic_overwrite = true)*/ OVERWRITE {{ mapping.starrocks_somatic_cnv_occurrence }}
WITH cytoband AS (SELECT o.name, o.seq_id, array_agg(c.cytoband) AS cytoband
                  FROM {{ mapping.starrocks_somatic_cnv_occurrence }} o
                  JOIN {{ mapping.starrocks_cytoband }} c ON c.chromosome = o.chromosome AND c.start <= o.end AND c.end >= o.start
                  WHERE o.part = %(part)s
                  GROUP BY o.name, o.seq_id),
     genes AS (SELECT o.name, o.seq_id, array_agg(g.name) AS symbol
               FROM {{ mapping.starrocks_somatic_cnv_occurrence }} o
               JOIN {{ mapping.starrocks_ensembl_gene }} g ON g.chromosome = o.chromosome AND g.start <= o.end
                    AND g.end >= o.start
               WHERE o.part = %(part)s
               GROUP BY o.name, o.seq_id),
     -- Position first, then overlap on (seq_id, chromosome, bin). Occurrence and variant are colocated on locus_id,
     -- so `snv_pos` is a local join. RAD-61: keyed on (seq_id, chromosome) alone, every CNV still met every SNV of its
     -- chromosome before the interval filter, and the planner, with no stats on the CNV side, could pick `snv_pos` as
     -- the hash table: a 166-sample WGS part filled the BE spill disk. Each CNV now has one row per 1 Mb bin it covers,
     -- so an SNV meets only the CNVs of its own bin, and exactly one row of each CNV covering it (nb_snv unchanged).
     -- The CNV bins are the small side: broadcast them as the hash table, never `snv_pos`.
     snv_pos AS (SELECT s.tumor_seq_id AS seq_id, s.locus_id, v.chromosome, v.start, v.start DIV 1000000 AS bin
                 FROM {{ mapping.starrocks_somatic_snv_occurrence }} s
                 JOIN {{ mapping.starrocks_snv_variant }} v ON v.locus_id = s.locus_id
                 WHERE s.part = %(part)s
                   -- RAD-57: same quality gate as the frequencies, so nb_snv counts quality-passing SNVs only.
                   AND s.filter = 'PASS' AND s.tumor_ad_alt >= 2),
     cnv_bin AS (SELECT o.name, o.seq_id, o.chromosome, o.start, o.end, b.bin
                 FROM {{ mapping.starrocks_somatic_cnv_occurrence }} o,
                      unnest(array_generate(o.start DIV 1000000, o.end DIV 1000000)) AS b(bin)
                 WHERE o.part = %(part)s),
     snv AS (SELECT c.name, c.seq_id, COUNT(DISTINCT p.locus_id) AS nb_snv
             FROM snv_pos p
             JOIN [BROADCAST] cnv_bin c ON c.seq_id = p.seq_id AND c.chromosome = p.chromosome AND c.bin = p.bin
                    AND p.start >= c.start AND p.start <= c.end
             GROUP BY c.name, c.seq_id),
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
        FROM {{ mapping.starrocks_somatic_cnv_occurrence }} cnv
        JOIN {{ mapping.iceberg_gnomad_sv }} gnomad
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
       cytoband.cytoband, genes.symbol, array_length(genes.symbol) AS nb_genes,
       snv.nb_snv,
       gnomad_ranked.af AS gnomad_af,
       gnomad_ranked.sc AS gnomad_sc,
       gnomad_ranked.sn AS gnomad_sn,
       gnomad_ranked.sf AS gnomad_sf,
       gnomad_ranked.sc_hom AS gnomad_sc_hom,
       gnomad_ranked.sc_het AS gnomad_sc_het
FROM {{ mapping.starrocks_somatic_cnv_occurrence }} o
         LEFT JOIN cytoband ON cytoband.seq_id = o.seq_id AND o.name = cytoband.name
         LEFT JOIN genes ON genes.seq_id = o.seq_id AND o.name = genes.name
         LEFT JOIN snv ON snv.seq_id = o.seq_id AND o.name = snv.name
         LEFT JOIN gnomad_ranked ON gnomad_ranked.seq_id = o.seq_id AND o.name = gnomad_ranked.name AND gnomad_ranked.rn = 1
WHERE o.part = %(part)s
;
