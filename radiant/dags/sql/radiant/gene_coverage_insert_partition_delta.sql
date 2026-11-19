-- RAD-315. The CDS rows of the files loaded by this run, with the gene name rule (the QLIN rule, clin-variant-etl
-- `enriched/CoverageByGene.scala`, without the RefSeq coordinates):
--   `SHOX_chrX` / `SHOX_chrY`   symbol without the suffix, chromosome X / Y from the suffix
--   `ENSG...`                   symbol = ensembl_gene.name matched on gene_id, else the ENSG id
--   plain symbol                symbol as is, chromosome from ensembl_gene matched on name
-- `gene` keeps the raw name. `ensembl_gene.chromosome` has no `chr` prefix. A name on more than one chromosome in
-- ensembl_gene keeps one row, the first by chromosome, so that the join never multiplies a gene.
-- The column order is the one of `gene_coverage`: the insert is positional.
WITH coverage AS (
    SELECT part,
           seq_id,
           task_id,
           gene,
           CASE WHEN gene REGEXP '_chr[XY]$' THEN RIGHT(gene, 1) END                AS suffix_chromosome,
           CASE WHEN gene REGEXP '_chr[XY]$' THEN LEFT(gene, LENGTH(gene) - 5) END AS suffix_symbol,
           gene REGEXP '^ENSG[0-9]+$'                                              AS is_ensembl_id,
           size,
           average_coverage,
           coverage_5x,
           coverage_15x,
           coverage_30x,
           coverage_50x,
           coverage_100x,
           coverage_200x,
           coverage_300x,
           coverage_400x,
           coverage_500x,
           coverage_1000x
    FROM {{ mapping.starrocks_staging_gene_coverage }}
    WHERE part = {{ partition }}
      AND tenant_code = %(tenant_code)s
      AND (seq_id IN %(seq_ids)s OR seq_id IN %(deleted_seq_ids)s)
      AND type = 'CDS'
),
gene_by_name AS (
    SELECT name, chromosome, gene_id
    FROM (
        SELECT name, chromosome, gene_id,
               ROW_NUMBER() OVER (PARTITION BY name ORDER BY chromosome, gene_id) AS rn
        FROM {{ mapping.starrocks_ensembl_gene }}
        WHERE name IS NOT NULL
    ) g
    WHERE rn = 1
),
gene_by_name_chromosome AS (
    SELECT name, chromosome, gene_id
    FROM (
        SELECT name, chromosome, gene_id,
               ROW_NUMBER() OVER (PARTITION BY name, chromosome ORDER BY gene_id) AS rn
        FROM {{ mapping.starrocks_ensembl_gene }}
        WHERE name IS NOT NULL AND chromosome IN ('X', 'Y')
    ) g
    WHERE rn = 1
),
gene_by_id AS (
    SELECT gene_id, name, chromosome
    FROM (
        SELECT gene_id, name, chromosome,
               ROW_NUMBER() OVER (PARTITION BY gene_id ORDER BY chromosome) AS rn
        FROM {{ mapping.starrocks_ensembl_gene }}
        WHERE gene_id IS NOT NULL
    ) g
    WHERE rn = 1
)
SELECT c.part,
       c.seq_id,
       c.task_id,
       CASE
           WHEN c.suffix_chromosome IS NOT NULL THEN c.suffix_symbol
           WHEN c.is_ensembl_id THEN COALESCE(gi.name, c.gene)
           ELSE c.gene
       END AS symbol,
       CASE
           WHEN c.suffix_chromosome IS NOT NULL THEN c.suffix_chromosome
           WHEN c.is_ensembl_id THEN gi.chromosome
           ELSE gn.chromosome
       END AS chromosome,
       c.gene,
       CASE
           WHEN c.suffix_chromosome IS NOT NULL THEN gnc.gene_id
           WHEN c.is_ensembl_id THEN c.gene
           ELSE gn.gene_id
       END AS ensembl_gene_id,
       c.size,
       c.average_coverage,
       c.coverage_5x,
       c.coverage_15x,
       c.coverage_30x,
       c.coverage_50x,
       c.coverage_100x,
       c.coverage_200x,
       c.coverage_300x,
       c.coverage_400x,
       c.coverage_500x,
       c.coverage_1000x
-- Each lookup has one row per key (rn = 1), so no join multiplies a row; the CASE above picks the lookup of the case.
FROM coverage c
LEFT JOIN gene_by_id gi ON gi.gene_id = c.gene
LEFT JOIN gene_by_name gn ON gn.name = c.gene
LEFT JOIN gene_by_name_chromosome gnc ON gnc.name = c.suffix_symbol AND gnc.chromosome = c.suffix_chromosome;
