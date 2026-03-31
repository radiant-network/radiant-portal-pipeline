"""
Pseudo-code for transcript annotation table open data (expression data) following the proposed schema:
https://docs.google.com/spreadsheets/d/18znLGx23qknUWXaqgMQ5cRNK3JZfUU9A7LwwdsGvfis/edit?usp=sharing

Annotation file:
gencode.v39.primary_assembly.annotation.gtf
"""

INSERT OVERWRITE {{ mapping.starrocks_gencode_transcript }}
SELECT
    source AS annotation_id,
    element_at(attributes, 'gene_id') AS gene_id,
    element_at(attributes, 'gene_name') AS gene_name,
    COALESCE(element_at(attributes, 'hugo_symbol'), element_at(attributes, 'gene_name')) AS hugo_symbol,
    element_at(attributes, 'gene_type') AS gene_type,
    seqname AS chrom,
    start AS gene_start,
    "end" AS gene_end,
    strand AS strand,
    phase,
    element_at(attributes, 'transcript_id') AS transcript_id,
    element_at(attributes, 'transcript_name') AS transcript_name,
    element_at(attributes, 'transcript_type') AS transcript_type,
    element_at(attributes, 'transcript_status') AS transcript_status,
    element_at(attributes, 'exon_number') AS exon_number,
    element_at(attributes, 'exon_id') AS exon_id,
    element_at(attributes, 'level') AS level,
    element_at(attributes, 'tag') AS tag,
    element_at(attributes, 'ccdsid') AS ccdsid,
    element_at(attributes, 'havana_gene') AS havana_gene,
    element_at(attributes, 'protein_id') AS protein_id,
    element_at(attributes, 'ont') AS ont,
    element_at(attributes, 'transcript_support_level') AS transcript_support_level,
    element_at(attributes, 'hgnc_id') AS hgnc_id
FROM {{ mapping.iceberg_gencode_gtf }};