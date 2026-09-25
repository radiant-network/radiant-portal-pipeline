INSERT OVERWRITE {{ mapping.starrocks_ensembl_exon_by_gene }}
{% if mapping.iceberg_ensembl_exon_by_gene_is_contract %}
-- ensembl_exon_by_gene_v1 does not carry phase, alias, description, external_name or logic_name.
SELECT  gene_id,
        exon_id,
        chromosome,
        start,
        end,
        transcript_ids,
        version,
        type,
        strand,
        CAST(NULL AS tinyint) AS phase,
        name,
        CAST(NULL AS array<varchar(128)>) AS alias,
        constitutive,
        CAST(NULL AS varchar(500)) AS description,
        ensembl_end_phase,
        ensembl_phase,
        CAST(NULL AS varchar(128)) AS external_name,
        CAST(NULL AS varchar(500)) AS logic_name,
        length
FROM {{ mapping.iceberg_ensembl_exon_by_gene }}
WHERE exon_id IS NOT NULL
{% else %}
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
{% endif %}
;
