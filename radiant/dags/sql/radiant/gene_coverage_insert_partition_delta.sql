-- RAD-315. The CDS rows of the files loaded by this run, with the gene name rule of QLIN (clin-variant-etl
-- `enriched/CoverageByGene.scala`, without the RefSeq coordinates):
--   `<name>_chr<contig>`        symbol without the suffix, chromosome from the suffix. DRAGEN adds the suffix to a
--                               name on more than one contig: the PAR genes (`SHOX_chrX`, `SHOX_chrY`), repeated
--                               ncRNAs (`_chr1`..`_chr22`), and so `_chrM` too; QLIN's pattern also takes `_chrMT`.
--   `ENSG...`                   symbol = ensembl_gene.name of the id (version dropped), else the raw name
--   plain symbol                symbol as is, chromosome from ensembl_gene
-- The Ensembl id of a name is the one of its first chromosome in string order ('11' < '2', 'X' < 'Y'): one row per
-- name, so the join never multiplies a gene, and both PAR rows get the X gene's id, as in QLIN. The mitochondrial
-- chromosome is `M` (ensembl_gene and a `_chrMT` suffix say `MT`), as in the variant tables. The insert is positional:
-- the column order is the one of `gene_coverage`.
WITH coverage AS (
    SELECT *,
           REGEXP_REPLACE(gene, '_chr([0-9]+|X|Y|M|MT)$', '')                            AS name,
           -- REGEXP_EXTRACT gives '' when the name has no suffix.
           NULLIF(REGEXP_EXTRACT(gene, '_chr([0-9]+|X|Y|M|MT)$', 1), '')                AS suffix_chromosome,
           IF(gene REGEXP '^ENSG[0-9]+([.][0-9]+)?$', SPLIT_PART(gene, '.', 1), NULL)     AS ensembl_id
    FROM {{ mapping.starrocks_staging_gene_coverage }}
    WHERE part = {{ partition }}
      AND tenant_code = %(tenant_code)s
      AND (seq_id IN %(seq_ids)s OR seq_id IN %(deleted_seq_ids)s)
      AND type = 'CDS'
),
ensembl AS (
    SELECT gene_id, name, IF(chromosome = 'MT', 'M', chromosome) AS chromosome,
           ROW_NUMBER() OVER (PARTITION BY name ORDER BY chromosome, gene_id) AS name_rank
    FROM {{ mapping.starrocks_ensembl_gene }}
    WHERE name IS NOT NULL
)
SELECT c.part,
       c.seq_id,
       c.task_id,
       COALESCE(by_id.name, c.name)                                AS symbol,
       COALESCE(IF(c.suffix_chromosome = 'MT', 'M', c.suffix_chromosome), by_id.chromosome, by_name.chromosome)
                                                                   AS chromosome,
       c.gene,
       COALESCE(c.ensembl_id, by_name.gene_id)                     AS ensembl_gene_id,
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
FROM coverage c
LEFT JOIN ensembl by_id ON by_id.gene_id = c.ensembl_id
LEFT JOIN ensembl by_name ON by_name.name = c.name AND by_name.name_rank = 1;
