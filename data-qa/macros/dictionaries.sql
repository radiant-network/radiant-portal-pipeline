{#
  Single source of truth for the accepted-values dictionaries used by the
  `accepted_values` / `accepted_values_in_array` tests across the source
  YAMLs. Each macro returns one closed list; reference it from a test with
  `values: "{{ dict_<name>() }}"`.

  Each list mirrors the portal's hardcoded closed sets — backend Go facets
  (`backend/internal/repository/facets.go`) and/or frontend i18n
  (`frontend/translations/common/*.json`). The `mirrors … — keep in sync`
  note lives here, once per dictionary, so there is a single place to update
  when an upstream value is added. Values are raw / case-sensitive (no
  normalization) — see data-qa/README.md.
#}

{% macro dict_chromosome() %}
  {# Notion: https://app.notion.com/p/ferlab/550fc21aed7a4de7a3636e80c6b3978a?v=2490f606225d4f60afe5088cdc77049c #}
  {# mirrors facets.go — keep in sync #}
  {{ return(['1','2','3','4','5','6','7','8','9','10','11','12',
             '13','14','15','16','17','18','19','20','21','22',
             'X','Y','M']) }}
{% endmacro %}

{% macro dict_vep_impact() %}
  {# Notion: https://app.notion.com/p/ferlab/08b7fcfc5a25414badffb256709d6521?v=59e0ecb4229d4bc08ddc538f76ae870f #}
  {# mirrors impact-indicator.tsx + facets.go — keep in sync #}
  {{ return(['HIGH', 'MODERATE', 'LOW', 'MODIFIER']) }}
{% endmacro %}

{% macro dict_variant_class() %}
  {# Notion: https://app.notion.com/p/ferlab/8baeb6e84ee24f0788a8154f8bfecdd9?v=6a806497b9d246d1b98def1df974e7c2 #}
  {# mirrors facets.go + i18n — keep in sync #}
  {{ return(['insertion', 'deletion', 'SNV', 'indel',
             'substitution', 'sequence_alteration']) }}
{% endmacro %}

{% macro dict_clinvar_interpretation() %}
  {# Notion: https://app.notion.com/p/ferlab/79b61815da5c4fbb898d65a6e88b5256?v=af0f09733b8345d3a68ee75dafabecfb #}
  {# mirrors facets.go + classification-badge.tsx + i18n — keep in sync #}
  {{ return(['Affects', 'association_not_found', 'association', 'Benign',
             'confers_sensitivity', 'Conflicting_classifications_of_pathogenicity',
             'conflicting_data_from_submitters',
             'Conflicting_interpretations_of_pathogenicity', 'drug_response',
             'Established_risk_allele', 'Likely_benign',
             'likely_pathogenic_low_penetrance', 'Likely_pathogenic',
             'Likely_risk_allele',
             'no_classification_for_the_single_variant', 'not_provided', 'other',
             'pathogenic_low_penetrance', 'Pathogenic', 'protective', 'risk_factor',
             'Uncertain_risk_allele', 'Uncertain_significance', '_low_penetrance']) }}
{% endmacro %}

{% macro dict_consequences() %}
  {# Notion: https://app.notion.com/p/ferlab/faa862446e4d4498a7238bd55e468b4f?v=2de0eadfa7d34ed49aa171c57e6e6c7e #}
  {# mirrors facets.go + i18n — keep in sync #}
  {{ return(['3_prime_UTR_variant', '5_prime_UTR_variant',
             'coding_sequence_variant', 'downstream_gene_variant',
             'feature_elongation', 'feature_truncation', 'frameshift_variant',
             'incomplete_terminal_codon_variant', 'inframe_deletion',
             'inframe_insertion', 'intergenic_variant', 'intron_variant',
             'mature_miRNA_variant', 'missense_variant', 'NMD_transcript_variant',
             'non_coding_transcript_exon_variant', 'non_coding_transcript_variant',
             'protein_altering_variant', 'regulatory_region_ablation',
             'regulatory_region_amplification', 'regulatory_region_variant',
             'splice_acceptor_variant', 'splice_donor_5th_base_variant',
             'splice_donor_region_variant', 'splice_donor_variant',
             'splice_polypyrimidine_tract_variant', 'splice_region_variant',
             'start_lost', 'start_retained_variant', 'stop_gained', 'stop_lost',
             'stop_retained_variant', 'synonymous_variant', 'TF_binding_site_variant',
             'TFBS_ablation', 'TFBS_amplification', 'transcript_ablation',
             'transcript_amplification', 'upstream_gene_variant']) }}
{% endmacro %}

{% macro dict_zygosity() %}
  {# Notion: https://app.notion.com/p/ferlab/b60cce4406dc460796882a8551454e27?v=30cfb82fd215434493c541a9b72d2dc0 #}
  {# mirrors i18n — keep in sync #}
  {{ return(['HET', 'HOM', 'HEM', 'WT', 'UNK']) }}
{% endmacro %}

{% macro dict_transmission_mode() %}
  {# Notion: https://app.notion.com/p/ferlab/2ef5a320b986493694e8a820d467d017?v=2808ef9b129c443e853d939a5524805d #}
  {# mirrors facets.go + transmission-mode-badge.tsx — keep in sync #}
  {{ return(['autosomal_dominant_de_novo', 'autosomal_dominant', 'autosomal_recessive',
             'x_linked_dominant_de_novo', 'x_linked_recessive_de_novo', 'x_linked_dominant',
             'x_linked_recessive', 'non_carrier_proband', 'unknown_parents_genotype',
             'unknown_father_genotype', 'unknown_mother_genotype', 'unknown_proband_genotype']) }}
{% endmacro %}

{% macro dict_exomiser_acmg_classification() %}
  {# Notion: https://app.notion.com/p/ferlab/1135448b7a5543e19c968fea72ea82e7?v=50ac6d7244744fc1a6b3c30347acd5e4 #}
  {# mirrors classification-badge.tsx + i18n — keep in sync #}
  {{ return(['pathogenic', 'likely_pathogenic', 'uncertain_significance',
             'likely_benign', 'benign']) }}
{% endmacro %}

{% macro dict_biotype() %}
  {# Notion: https://app.notion.com/p/ferlab/e6270b2666794b23876ff4fbcc3257fe?v=5adc1d6fe362491586e6a2feba0d8aa9 #}
  {# mirrors facets.go + i18n — keep in sync #}
  {{ return(['IG_C_gene', 'IG_D_gene', 'IG_J_gene', 'IG_LV_gene', 'IG_V_gene',
             'TR_C_gene', 'TR_J_gene', 'TR_V_gene', 'TR_D_gene', 'IG_pseudogene',
             'IG_C_pseudogene', 'IG_J_pseudogene', 'IG_V_pseudogene', 'TR_V_pseudogene',
             'TR_J_pseudogene', 'Mt_rRNA', 'Mt_tRNA', 'miRNA', 'misc_RNA', 'rRNA',
             'scRNA', 'snRNA', 'snoRNA', 'ribozyme', 'sRNA', 'scaRNA', 'lncRNA',
             'Mt_tRNA_pseudogene', 'tRNA_pseudogene', 'snoRNA_pseudogene',
             'snRNA_pseudogene', 'scRNA_pseudogene', 'rRNA_pseudogene',
             'misc_RNA_pseudogene', 'miRNA_pseudogene', 'TEC', 'nonsense_mediated_decay',
             'non_stop_decay', 'retained_intron', 'protein_coding', 'protein_coding_LoF',
             'protein_coding_CDS_not_defined', 'processed_transcript', 'non_coding',
             'ambiguous_orf', 'sense_intronic', 'sense_overlapping', 'antisense_RNA',
             'known_ncrna', 'pseudogene', 'processed_pseudogene', 'polymorphic_pseudogene',
             'retrotransposed', 'transcribed_processed_pseudogene',
             'transcribed_unprocessed_pseudogene', 'transcribed_unitary_pseudogene',
             'translated_processed_pseudogene', 'translated_unprocessed_pseudogene',
             'unitary_pseudogene', 'unprocessed_pseudogene', 'artifact', 'lincRNA',
             'macro_lncRNA', '3prime_overlapping_ncRNA', 'disrupted_domain', 'vault_RNA',
             'bidirectional_promoter_lncRNA', '']) }}
{% endmacro %}

{% macro dict_sift_pred() %}
  {# Notion: https://app.notion.com/p/ferlab/95e8abf677f146838781fec28133ab3d?v=f2bcec55623e464eb049b75122517566 #}
  {# mirrors i18n — keep in sync #}
  {{ return(['D', 'T']) }}
{% endmacro %}

{% macro dict_polyphen2_hvar_pred() %}
  {# Notion: https://app.notion.com/p/ferlab/09cfb48b28d3453cbef8c5f382c782a5?v=82c36d10c03c43d48694a54d4ed0d87f #}
  {# mirrors i18n — keep in sync #}
  {{ return(['B', 'D', 'P']) }}
{% endmacro %}

{% macro dict_fathmm_pred() %}
  {# Notion: https://app.notion.com/p/ferlab/c83664a3c1ce4c2dbddcdf592516ee77?v=4f747ab3a72d4c47b92b0ea640b91ba5 #}
  {# mirrors i18n — keep in sync #}
  {{ return(['D', 'T']) }}
{% endmacro %}

{% macro dict_lrt_pred() %}
  {# Notion: https://app.notion.com/p/ferlab/1a62cfbdf86c48559752f011ab20903c?v=93a01edfa8a747d3a66c8efc67ec8bb9 #}
  {# mirrors facets.go + i18n — keep in sync #}
  {{ return(['D', 'N', 'U']) }}
{% endmacro %}
