INSERT OVERWRITE {{ mapping.starrocks_ensembl_exon_by_gene }}
SELECT  gene_id,
        exon_id,
        chromosome,
        start,
        end,
        transcript_ids,
        version,
        type,
        strand,
        phase,
        name,
        alias,
        constitutive,
        description,
        ensembl_end_phase,
        ensembl_phase,
        external_name,
        logic_name,
        length
FROM {{ mapping.iceberg_ensembl_exon_by_gene }}
WHERE exon_id IS NOT NULL
;

