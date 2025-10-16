
TRUNCATE {{ params.clinical_organization }} CASCADE;
INSERT INTO {{ params.clinical_organization }} (id, code, name, category_code) VALUES
    (10000001, 'CHOP', 'The Children''s Hospital of Philadelphia', 'healthcare_provider'),
    (10000002, 'Pitt', 'University of Pittsburgh', 'healthcare_provider')
;

TRUNCATE {{ params.clinical_patient }} CASCADE;
INSERT INTO {{ params.clinical_patient }} (id, mrn, managing_organization_id, sex_code, date_of_birth) VALUES
    (1000001, '3492585', 10000001, 'male', '2017-09-17')
;

-- Cases
TRUNCATE {{ params.clinical_project }} CASCADE;
INSERT INTO {{ params.clinical_project }} (id, code, name, description) VALUES
    (10000001, 'CBTN', 'Children''s Brain Tumor Network', NULL)
;

TRUNCATE {{ params.clinical_request }} CASCADE;
INSERT INTO {{ params.clinical_request }} (id, priority_code, ordering_physician, ordering_organization_id, order_number) VALUES
    (1, 'routine', 'Felix Laflamme', 10000001, '25850340'),
    (2, 'routine', 'Melissa Lopez', 10000001, '25850341'),
    (3, 'routine', 'Christopher Watson', 10000001, '25850342'),
    (4, 'routine', 'Victoria Breton', 10000001, '25850343'),
    (5, 'routine', 'Antoine Paré', 10000001, '25850344'),
    (6, 'routine', 'Jonathan Frye', 10000001, '25850345')
;

TRUNCATE {{ params.clinical_case_analysis }} CASCADE;
INSERT INTO {{ params.clinical_case_analysis }} (id, code, name, type_code, panel_id, description)
VALUES (10000001, 'variant', 'variant calling', 'somatic', NULL, NULL)
;

TRUNCATE {{ params.clinical_case }} CASCADE;
INSERT INTO {{ params.clinical_case }} (id, proband_id, project_id, case_analysis_id, status_code, primary_condition, request_id, performer_lab_id, note, created_on, updated_on) VALUES
    (1000001, 1000001, 10000001, 10000001, 'completed', 'MONDO:0021248', 1, 10000001,  'Administrative comment', '2025-09-16T18:48:19Z', '2025-09-16T18:48:19Z')
;

TRUNCATE {{ params.clinical_family }} CASCADE;
INSERT INTO {{ params.clinical_family }} (id, case_id, family_member_id, relationship_to_proband_code, affected_status_code) VALUES
    (1, 1000001, 1000001, 'mother', 'affected'),
    (2, 1000001, 1000001, 'father', 'non_affected')
;


-- Observations
TRUNCATE {{ params.clinical_observation_coding }} CASCADE;
INSERT INTO {{ params.clinical_observation_coding }} (
    id,
    case_id,
    patient_id,
    observation_code,
    coding_system,
    code_value,
    onset_code,
    interpretation_code,
    note
) VALUES
      (1000001, 1000001, 1000001, 'phenotype', 'HPO', 'HP:0002898', 'childhood', 'positive', NULL)
;


-- Samples
TRUNCATE {{ params.clinical_sample }} CASCADE;
INSERT INTO {{ params.clinical_sample }} (id, category_code, type_code, parent_sample_id, tissue_site, histology_code, submitter_sample_id) VALUES
(1000001, 'sample', 'dna', NULL, 'Frontal Lobe; Other locations NOS; Parietal Lobe; Spine NOS', 'tumoral', '1273283'),
(1000002, 'sample', 'dna', NULL, 'Frontal Lobe; Other locations NOS; Parietal Lobe; Spine NOS', 'tumoral', '1273009'),
(1000003, 'sample', 'dna', NULL, 'Frontal Lobe; Other locations NOS; Parietal Lobe; Spine NOS', 'tumoral', '1273010')
;


-- Sequencing Experiment
TRUNCATE {{ params.clinical_experiment }} CASCADE;
INSERT INTO {{ params.clinical_experiment }} (id, code, name, experimental_strategy_code, platform_code, description)
VALUES (1000001, 'wgs_illumina', 'WGS Illumina', 'wgs', 'illumina', NULL)
;

TRUNCATE {{ params.clinical_sequencing_experiment }} CASCADE;
INSERT INTO {{ params.clinical_sequencing_experiment }} (id, case_id, patient_id, sample_id, experiment_id, status_code, aliquot, request_id, performer_lab_id, run_name, run_alias, run_date, capture_kit, is_paired_end, read_length, created_on, updated_on) VALUES
        (1000001, 1000001, 1000001, 1000003, 1000001, 'completed', 'BS_B9V9296D', NULL, NULL, NULL, NULL, '2022-11-08 00:00:00', NULL, '0', '151', '2023-01-27 22:14:51.619206', '2024-09-19 18:23:01.384835'),
        (1000002, 1000001, 1000001, 1000002, 1000001, 'completed', 'BS_9ANMMYJS', NULL, NULL, NULL, NULL, '2022-11-08 00:00:00', NULL, '0', '151', '2023-01-27 22:14:51.461801', '2024-09-19 18:22:50.647529'),
        (1000003, 1000001, 1000001, 1000001, 1000001, 'completed', 'BS_WX8Y5A0A', NULL, NULL, NULL, NULL, '2022-11-07 00:00:00', NULL, '0', '151', '2023-01-27 22:11:33.576251', '2024-09-19 18:22:16.014562')
;


-- Task
TRUNCATE {{ params.clinical_pipeline }} CASCADE;
INSERT INTO {{ params.clinical_pipeline }} (id, description, genome_build) VALUES
        (1000001, 'Source Data', 'GRCh38'),
        (1000002, 'Kids First DRC Germline Variant Workflows. internal_id: germline_v1.1.0', 'GRCh38'),
        (1000003, 'Kids First DRC Germline Variant Workflows. internal_id: germline_v0.3.0', 'GRCh38'),
        (1000004, 'Kids First DRC Single Sample Genotyping Workflow. internal_id: germline_v1.1.0', 'GRCh38'),
        (1000005, 'Kids First DRC Somatic Variant Workflow. internal_id: somatic_v4.4.0', 'GRCh38'),
        (1000006, 'Kids First DRC Somatic Variant Workflow. internal_id: somatic_v5.0.0_wf_improvement', 'GRCh38'),
        (1000007, 'Kids First DRC Alignment and GATK HaplotypeCaller Workflow. internal_id: alignment_v2.7.0_wf_improvement', 'GRCh38'),
        (1000008, 'Kids First DRC Alignment and GATK HaplotypeCaller Workflow. internal_id: alignment_v2.9.0_wf_improvement', 'GRCh38')
;

TRUNCATE {{ params.clinical_task }} CASCADE;
INSERT INTO {{ params.clinical_task }} (id, type_code, pipeline_id, created_on) VALUES
    (1000001, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000002, 'ngba', 1000004, '2021-10-12 13:08:00'),
    (1000003, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000004, 'ngba', 1000001, '2021-10-12 13:08:00'),
    (1000005, 'ngba', 1000005, '2021-10-12 13:08:00'),
    (1000006, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000007, 'ngba', 1000008, '2021-10-12 13:08:00'),
    (1000008, 'ngba', 1000005, '2021-10-12 13:08:00'),
    (1000009, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000010, 'ngba', 1000002, '2021-10-12 13:08:00'),
    (1000011, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000012, 'ngba', 1000005, '2021-10-12 13:08:00'),
    (1000013, 'ngba', 1000003, '2021-10-12 13:08:00'),
    (1000014, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000015, 'ngba', 1000005, '2021-10-12 13:08:00'),
    (1000016, 'ngba', 1000001, '2021-10-12 13:08:00'),
    (1000017, 'ngba', 1000001, '2021-10-12 13:08:00'),
    (1000018, 'ngba', 1000001, '2021-10-12 13:08:00'),
    (1000019, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000020, 'ngba', 1000005, '2021-10-12 13:08:00'),
    (1000021, 'ngba', 1000007, '2021-10-12 13:08:00'),
    (1000022, 'ngba', 1000005, '2021-10-12 13:08:00'),
    (1000023, 'ngba', 1000001, '2021-10-12 13:08:00'),
    (1000024, 'ngba', 1000001, '2021-10-12 13:08:00'),
    (1000025, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000026, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000027, 'ngba', 1000008, '2021-10-12 13:08:00'),
    (1000028, 'ngba', 1000005, '2021-10-12 13:08:00'),
    (1000029, 'ngba', 1000006, '2021-10-12 13:08:00'),
    (1000030, 'ngba', 1000005, '2021-10-12 13:08:00')
;

TRUNCATE {{ params.clinical_task_has_sequencing_experiment }} CASCADE;
INSERT INTO {{ params.clinical_task_has_sequencing_experiment }} (
    task_id,
    sequencing_experiment_id
) VALUES
    (1000011, 1000003),
    (1000020, 1000002),
    (1000007, 1000002),
    (1000018, 1000001),
    (1000005, 1000003),
    (1000014, 1000001),
    (1000003, 1000002),
    (1000026, 1000003),
    (1000015, 1000002),
    (1000030, 1000002),
    (1000021, 1000003),
    (1000012, 1000003),
    (1000013, 1000003),
    (1000028, 1000003),
    (1000030, 1000003),
    (1000001, 1000003),
    (1000029, 1000003),
    (1000006, 1000003),
    (1000028, 1000001),
    (1000022, 1000002),
    (1000005, 1000002),
    (1000006, 1000002),
    (1000003, 1000003),
    (1000004, 1000003),
    (1000025, 1000003),
    (1000009, 1000001),
    (1000011, 1000001),
    (1000016, 1000002),
    (1000015, 1000003),
    (1000008, 1000001),
    (1000027, 1000001),
    (1000009, 1000003),
    (1000002, 1000003),
    (1000025, 1000002),
    (1000029, 1000001),
    (1000017, 1000003),
    (1000001, 1000002),
    (1000019, 1000003),
    (1000019, 1000002),
    (1000014, 1000003),
    (1000010, 1000003),
    (1000026, 1000001),
    (1000008, 1000003),
    (1000023, 1000001),
    (1000024, 1000002),
    (1000022, 1000003),
    (1000012, 1000001),
    (1000020, 1000003)
;

TRUNCATE {{ params.clinical_document }} CASCADE;
INSERT INTO {{ params.clinical_document }} (id, name, data_category_code, data_type_code, format_code, size, url, hash) VALUES
    (1000001, '073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000002, '073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000003, '073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.public.vcf.gz', NULL),
    (1000004, '073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/073018c7-dd1e-4749-86a6-0687947af740.mutect2_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000005, '08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000006, '08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000007, '08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.public.vcf.gz', NULL),
    (1000008, '08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/08b36fcb-bccf-4406-9c90-d6674c48d36f.vardict_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000009, '11f14903-b015-4085-9a9f-bd71bea6295d.g.vcf.gz', 'genomic', 'snv', 'vcf', '5504004765', 's3://cds-306-phs002517-x01/harmonized-data/raw-gvcf/11f14903-b015-4085-9a9f-bd71bea6295d.g.vcf.gz', '9ca325f67b37e2964847c7b660e602d2'),
    (1000010, '11f14903-b015-4085-9a9f-bd71bea6295d.g.vcf.gz', 'genomic', 'snv', 'gvcf', '5504004765', 's3://cds-306-phs002517-x01/harmonized-data/raw-gvcf/11f14903-b015-4085-9a9f-bd71bea6295d.g.vcf.gz', '9ca325f67b37e2964847c7b660e602d2'),
    (1000011, '11f14903-b015-4085-9a9f-bd71bea6295d.g.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '1344233', 's3://cds-306-phs002517-x01/harmonized-data/raw-gvcf/11f14903-b015-4085-9a9f-bd71bea6295d.g.vcf.gz.tbi', 'f0288d122a06d9aae49fc58e1ecdca5e'),
    (1000012, '1273009_D1.cram', 'genomic', 'alignment', 'cram', '47700752356', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273009_D1/v2/1273009_D1.cram', 'd9647d87a4224226a8c62fc8c166145a'),
    (1000013, '1273009_D1.cram', 'genomic', 'exomiser', 'cram', '47700752356', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273009_D1/v2/1273009_D1.cram', 'd9647d87a4224226a8c62fc8c166145a'),
    (1000014, '1273009_D1.cram.crai', 'genomic', 'alignment', 'crai', '3972803', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273009_D1/v2/1273009_D1.cram.crai', '18a20f8b07b9bd59ce8a9b3da141221f'),
    (1000015, '1273009_D1.cram.crai', 'genomic', 'exomiser', 'crai', '3972803', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273009_D1/v2/1273009_D1.cram.crai', '18a20f8b07b9bd59ce8a9b3da141221f'),
    (1000016, '1273010_D1.cram', 'genomic', 'exomiser', 'cram', '35958877088', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273010_D1/v1/1273010_D1.cram', '57380318c3925103a829191d962724a9'),
    (1000017, '1273010_D1.cram', 'genomic', 'alignment', 'cram', '35958877088', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273010_D1/v1/1273010_D1.cram', '57380318c3925103a829191d962724a9'),
    (1000018, '1273010_D1.cram.crai', 'genomic', 'exomiser', 'crai', '3096124', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273010_D1/v1/1273010_D1.cram.crai', '49f484c7944b8808e5a49b6fe49a3144'),
    (1000019, '1273010_D1.cram.crai', 'genomic', 'alignment', 'crai', '3096124', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_60xTumor_7Nov2022/RP-2628/WGS/1273010_D1/v1/1273010_D1.cram.crai', '49f484c7944b8808e5a49b6fe49a3144'),
    (1000020, '1273283_D1.cram', 'genomic', 'exomiser', 'cram', '18534608844', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_30xTissue_7Nov2022/RP-2628/WGS/1273283_D1/v1/1273283_D1.cram', '0d3de9910a83e7c014f9cbc439850892'),
    (1000021, '1273283_D1.cram', 'genomic', 'alignment', 'cram', '18534608844', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_30xTissue_7Nov2022/RP-2628/WGS/1273283_D1/v1/1273283_D1.cram', '0d3de9910a83e7c014f9cbc439850892'),
    (1000022, '1273283_D1.cram.crai', 'genomic', 'alignment', 'crai', '1610967', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_30xTissue_7Nov2022/RP-2628/WGS/1273283_D1/v1/1273283_D1.cram.crai', '8a56bd4cad24752cb93370ba73beff40'),
    (1000023, '1273283_D1.cram.crai', 'genomic', 'exomiser', 'crai', '1610967', 's3://cds-306-phs002517-x01/source/GMKF_Resnick_WGS_30xTissue_7Nov2022/RP-2628/WGS/1273283_D1/v1/1273283_D1.cram.crai', '8a56bd4cad24752cb93370ba73beff40'),
    (1000024, '3a66f6af-6e99-4515-87b5-bdfb32e5c0a1_genotype.tsv', 'genomic', 'exomiser', 'tsv', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/hla-genotyping/3a66f6af-6e99-4515-87b5-bdfb32e5c0a1_genotype.tsv', NULL),
    (1000025, '3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.merged.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.merged.vcf.gz', NULL),
    (1000026, '3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.merged.vcf.gz.tbi', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.merged.vcf.gz.tbi', NULL),
    (1000027, '3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000028, '3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000029, '3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.public.vcf.gz', NULL),
    (1000030, '3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/3fdeae44-3fcd-4910-a691-ee38e9ffe773.lancet_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000031, '450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000032, '450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000033, '450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.public.vcf.gz', NULL),
    (1000034, '450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/450ecc10-d171-403f-becf-633fe3b65601.strelka2_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000035, '46de61d6-135e-464b-9dc1-8290a52543a2.BEST.results', 'genomic', 'exomiser', 'tsv', '63334', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/46de61d6-135e-464b-9dc1-8290a52543a2.BEST.results', 'df0c53521bddd2dcb3a399d326cfafd9'),
    (1000036, '46de61d6-135e-464b-9dc1-8290a52543a2.mutect2_filtered.merged.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '29748881', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/46de61d6-135e-464b-9dc1-8290a52543a2.mutect2_filtered.merged.reheadered.vcf.gz', '975aa66ae29e9a8b92c00f93259c3d00'),
    (1000037, '46de61d6-135e-464b-9dc1-8290a52543a2.mutect2_filtered.merged.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '825785', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/46de61d6-135e-464b-9dc1-8290a52543a2.mutect2_filtered.merged.reheadered.vcf.gz.tbi', '30a387efd9843da2083ceecff0fbb2d7'),
    (1000038, '46de61d6-135e-464b-9dc1-8290a52543a2.n2.results', 'genomic', 'exomiser', 'tsv', '63334', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/46de61d6-135e-464b-9dc1-8290a52543a2.n2.results', 'df0c53521bddd2dcb3a399d326cfafd9'),
    (1000039, '46de61d6-135e-464b-9dc1-8290a52543a2.vardict_somatic.merged.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '624537463', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/46de61d6-135e-464b-9dc1-8290a52543a2.vardict_somatic.merged.reheadered.vcf.gz', 'db3085b7f2760b7d9ed7a83c5d55762d'),
    (1000040, '46de61d6-135e-464b-9dc1-8290a52543a2.vardict_somatic.merged.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '1798682', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/46de61d6-135e-464b-9dc1-8290a52543a2.vardict_somatic.merged.reheadered.vcf.gz.tbi', 'fa067732788753ffe1b65db0f0974e99'),
    (1000041, '4b5628a5-1612-4823-aa77-8fe623daee06.manta.candidateSmallIndels.vcf.gz', 'genomic', 'snv', 'vcf', '3145172', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.manta.candidateSmallIndels.vcf.gz', '6338f4a6b7cdf848fca8111099765968'),
    (1000042, '4b5628a5-1612-4823-aa77-8fe623daee06.manta.candidateSmallIndels.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '666503', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.manta.candidateSmallIndels.vcf.gz.tbi', '71d5951956f733aae6025438dc7bf25f'),
    (1000043, '4b5628a5-1612-4823-aa77-8fe623daee06.manta.diploidSV.annotated.tsv', 'genomic', 'exomiser', 'tsv', '17622068', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.manta.diploidSV.annotated.tsv', '5bc2f7c909759e54acf2ad5ac8ae2567'),
    (1000044, '4b5628a5-1612-4823-aa77-8fe623daee06.manta.diploidSV.vcf.gz', 'genomic', 'snv', 'vcf', '926519', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.manta.diploidSV.vcf.gz', '11f4419ce49e69f3e999a7148df9f5ae'),
    (1000045, '4b5628a5-1612-4823-aa77-8fe623daee06.manta.diploidSV.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '108472', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.manta.diploidSV.vcf.gz.tbi', '79b9714b787ac252715f637a82a20f22'),
    (1000046, '4b5628a5-1612-4823-aa77-8fe623daee06.svaba.indel.vcf.gz', 'genomic', 'snv', 'vcf', '31863313', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.svaba.indel.vcf.gz', 'ed2f1af073979e3a38c1468a5db0b8c8'),
    (1000047, '4b5628a5-1612-4823-aa77-8fe623daee06.svaba.sv.annotated.tsv', 'genomic', 'exomiser', 'tsv', '6963171', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.svaba.sv.annotated.tsv', '591bdb7198795062dc0d808aac054798'),
    (1000048, '4b5628a5-1612-4823-aa77-8fe623daee06.svaba.sv.vcf.gz', 'genomic', 'snv', 'vcf', '438602', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/germline-structure-variation/4b5628a5-1612-4823-aa77-8fe623daee06.svaba.sv.vcf.gz', '1d7b651ec3b63d40bb60fb5445d3f225'),
    (1000049, '4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000050, '4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000051, '4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.public.vcf.gz', NULL),
    (1000052, '4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/4ba4b83e-8e4d-418b-99c5-008c8883ce24.strelka2_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000053, '543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000054, '543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000055, '543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.public.vcf.gz', NULL),
    (1000056, '543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/543a59fe-293f-4a89-84e6-a20f8534ea25.consensus_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000057, '7d6337b9-87c7-4c4e-8202-1301682243c1_amplicon1_amplicon_classification_profiles.tsv', 'genomic', 'exomiser', 'tsv', '164', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/7d6337b9-87c7-4c4e-8202-1301682243c1_amplicon1_amplicon_classification_profiles.tsv', 'b7dde0d26ef896b0933c1af51c93d408'),
    (1000058, '7d6337b9-87c7-4c4e-8202-1301682243c1_amplicon1_gene_list.tsv', 'genomic', 'exomiser', 'tsv', '259', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/7d6337b9-87c7-4c4e-8202-1301682243c1_amplicon1_gene_list.tsv', '7931de77db0e992e0b6226592ffb9d73'),
    (1000059, '7d6337b9-87c7-4c4e-8202-1301682243c1.cnr', 'genomic', 'exomiser', 'tsv', '154342578', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/7d6337b9-87c7-4c4e-8202-1301682243c1.cnr', '60ab8a4d5b79ad2a0ca700c35ec3d38e'),
    (1000060, '7d6337b9-87c7-4c4e-8202-1301682243c1_cnvkit_reference.cnn', 'genomic', 'exomiser', 'tsv', '184765306', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/7d6337b9-87c7-4c4e-8202-1301682243c1_cnvkit_reference.cnn', 'c90f0d48d5991a7bb6bacb92bbd1396c'),
    (1000061, '7d6337b9-87c7-4c4e-8202-1301682243c1.gatk_cnv.funcotated.tsv', 'genomic', 'exomiser', 'tsv', '669457', 's3://cds-306-phs002517-x01/harmonized-data/copy-number-variations/7d6337b9-87c7-4c4e-8202-1301682243c1.gatk_cnv.funcotated.tsv', 'bc3c603ff8c451595d63ec1cea223b88'),
    (1000062, '7d6337b9-87c7-4c4e-8202-1301682243c1.manta.PASS.annotated.tsv', 'genomic', 'exomiser', 'tsv', '2687737', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/7d6337b9-87c7-4c4e-8202-1301682243c1.manta.PASS.annotated.tsv', '610e19de2b17ab07939d81753c255c2f'),
    (1000063, '7d6337b9-87c7-4c4e-8202-1301682243c1.manta.PASS.vcf.gz', 'genomic', 'snv', 'vcf', '39810', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/7d6337b9-87c7-4c4e-8202-1301682243c1.manta.PASS.vcf.gz', '111f16fd5c371eef4cf18d8e868b0117'),
    (1000064, '7d6337b9-87c7-4c4e-8202-1301682243c1.manta.PASS.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '16185', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/7d6337b9-87c7-4c4e-8202-1301682243c1.manta.PASS.vcf.gz.tbi', '9f35f12d9fadf87565039a6e72fe8907'),
    (1000065, '7d6337b9-87c7-4c4e-8202-1301682243c1.manta.somaticSV.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '51738', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/7d6337b9-87c7-4c4e-8202-1301682243c1.manta.somaticSV.reheadered.vcf.gz', '0c317d8099076d3684d9f12d291893ae'),
    (1000066, '7d6337b9-87c7-4c4e-8202-1301682243c1.manta.somaticSV.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '9887', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/7d6337b9-87c7-4c4e-8202-1301682243c1.manta.somaticSV.reheadered.vcf.gz.tbi', 'a9c4cc14765e16247438d1c328109474'),
    (1000067, '7d6337b9-87c7-4c4e-8202-1301682243c1.strelka2_somatic.merged.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '75103617', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/7d6337b9-87c7-4c4e-8202-1301682243c1.strelka2_somatic.merged.reheadered.vcf.gz', 'b1b24436643b2f50526db7dd143c5edb'),
    (1000068, '7d6337b9-87c7-4c4e-8202-1301682243c1.strelka2_somatic.merged.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '1323558', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/7d6337b9-87c7-4c4e-8202-1301682243c1.strelka2_somatic.merged.reheadered.vcf.gz.tbi', 'de829364e7e8145aa1714819fb5b53c9'),
    (1000069, '8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.merged.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.merged.vcf.gz', NULL),
    (1000070, '8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.merged.vcf.gz.tbi', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.merged.vcf.gz.tbi', NULL),
    (1000071, '8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000072, '8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000073, '8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.public.vcf.gz', NULL),
    (1000074, '8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/8366ce00-043c-4671-aef1-1bad562b4e10.lancet_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000075, '8984b835-3a51-4dd7-aad0-73d01548aeab.single.vqsr.filtered.vep_105.vcf.gz', 'genomic', 'snv', 'vcf', '1913602964', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/8984b835-3a51-4dd7-aad0-73d01548aeab.single.vqsr.filtered.vep_105.vcf.gz', '365403f160fc8876ddbcfb6a1e6d12c8'),
    (1000076, '8984b835-3a51-4dd7-aad0-73d01548aeab.single.vqsr.filtered.vep_105.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '2250279', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/8984b835-3a51-4dd7-aad0-73d01548aeab.single.vqsr.filtered.vep_105.vcf.gz.tbi', 'de1d72a9fb87f64230497b64faff6678'),
    (1000077, '9dc2e435-04d1-4486-b548-e9b5e0f5b360_genotype.tsv', 'genomic', 'exomiser', 'tsv', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/hla-genotyping/9dc2e435-04d1-4486-b548-e9b5e0f5b360_genotype.tsv', NULL),
    (1000078, 'a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000079, 'a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000080, 'a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.public.vcf.gz', NULL),
    (1000081, 'a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/a7affef9-de5e-41bc-a489-80cf264b97eb.consensus_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000082, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.cnr', 'genomic', 'exomiser', 'tsv', '154340683', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.cnr', '7f8e97dcf1e7d056a864d8f5e7215ca2'),
    (1000083, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543_cnvkit_reference.cnn', 'genomic', 'exomiser', 'tsv', '184765306', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543_cnvkit_reference.cnn', 'c90f0d48d5991a7bb6bacb92bbd1396c'),
    (1000084, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.gatk_cnv.funcotated.tsv', 'genomic', 'exomiser', 'tsv', '670991', 's3://cds-306-phs002517-x01/harmonized-data/copy-number-variations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.gatk_cnv.funcotated.tsv', '6d4ce840422131cf6750963efd443da9'),
    (1000085, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.PASS.annotated.tsv', 'genomic', 'exomiser', 'tsv', '3902270', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.PASS.annotated.tsv', '0106429dd0960953c1f44608cfe42617'),
    (1000086, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.PASS.vcf.gz', 'genomic', 'snv', 'vcf', '40025', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.PASS.vcf.gz', 'e83a7b5fc5d3e3e873496c8d6510f4b7'),
    (1000087, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.PASS.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '14138', 's3://cds-306-phs002517-x01/harmonized-data/structural-variations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.PASS.vcf.gz.tbi', 'f44f618b8fa1acde6d8b0158c237580f'),
    (1000088, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.somaticSV.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '42739', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.somaticSV.reheadered.vcf.gz', 'fca597e0c07f1b5ee7f02278c1d8f01c'),
    (1000089, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.somaticSV.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '7829', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.manta.somaticSV.reheadered.vcf.gz.tbi', '8df6ba784916e2a237477e6eb87df326'),
    (1000090, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.strelka2_somatic.merged.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '55639334', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.strelka2_somatic.merged.reheadered.vcf.gz', '05ce81424b70c83a96d974a36b6d4248'),
    (1000091, 'ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.strelka2_somatic.merged.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '1226331', 's3://cds-306-phs002517-x01/harmonized-data/workflow-outputs/somatic-mutations/ad3a3f5c-37b6-4203-ba2f-bdf2c3a6f543.strelka2_somatic.merged.reheadered.vcf.gz.tbi', 'd04d17d9a222b51ba666fc1b9a68cd18'),
    (1000092, 'b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000093, 'b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000094, 'b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.public.vcf.gz', NULL),
    (1000095, 'b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/b800ea77-9b28-4e84-a055-0035fd3c633d.mutect2_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000096, 'cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.protected.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.protected.vcf.gz', NULL),
    (1000097, 'cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.protected.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.protected.vcf.gz.tbi', NULL),
    (1000098, 'cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.public.vcf.gz', 'genomic', 'snv', 'vcf', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.public.vcf.gz', NULL),
    (1000099, 'cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.public.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '308994375', 's3://d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/cb2aae1c-0ec5-4581-a307-68db003fdddf.vardict_somatic.norm.annot.public.vcf.gz.tbi', NULL),
    (1000100, 'dc8769f0-a367-4b29-be70-6009c2179a0d.BEST.results', 'genomic', 'exomiser', 'tsv', '40610', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/dc8769f0-a367-4b29-be70-6009c2179a0d.BEST.results', '58abb2f4aecd36f649cc127262f10131'),
    (1000101, 'dc8769f0-a367-4b29-be70-6009c2179a0d.mutect2_filtered.merged.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '32485898', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/dc8769f0-a367-4b29-be70-6009c2179a0d.mutect2_filtered.merged.reheadered.vcf.gz', 'c7874292168aa5a310daa090e0b42bfd'),
    (1000102, 'dc8769f0-a367-4b29-be70-6009c2179a0d.mutect2_filtered.merged.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '816491', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/dc8769f0-a367-4b29-be70-6009c2179a0d.mutect2_filtered.merged.reheadered.vcf.gz.tbi', '03353e7c03f397e50b36dff75b515065'),
    (1000103, 'dc8769f0-a367-4b29-be70-6009c2179a0d.n2.results', 'genomic', 'exomiser', 'tsv', '40610', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/dc8769f0-a367-4b29-be70-6009c2179a0d.n2.results', '58abb2f4aecd36f649cc127262f10131'),
    (1000104, 'dc8769f0-a367-4b29-be70-6009c2179a0d.vardict_somatic.merged.reheadered.vcf.gz', 'genomic', 'snv', 'vcf', '633334336', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/dc8769f0-a367-4b29-be70-6009c2179a0d.vardict_somatic.merged.reheadered.vcf.gz', '029b1c2c969449a48d83b0d14797ecc8'),
    (1000105, 'dc8769f0-a367-4b29-be70-6009c2179a0d.vardict_somatic.merged.reheadered.vcf.gz.tbi', 'genomic', 'snv', 'tbi', '1799520', 's3://cds-246-phs002517-p30-fy20/d3b-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/workflow-outputs/somatic-mutations/dc8769f0-a367-4b29-be70-6009c2179a0d.vardict_somatic.merged.reheadered.vcf.gz.tbi', '555079f6ee9313447366ba7c1f1f8562')
;

TRUNCATE {{ params.clinical_task_has_document }} CASCADE;
INSERT INTO {{ params.clinical_task_has_document }} (task_id, document_id) VALUES
    (1000028, 1000088),
    (1000030, 1000058),
    (1000006, 1000072),
    (1000013, 1000042),
    (1000020, 1000059),
    (1000003, 1000033),
    (1000001, 1000053),
    (1000009, 1000095),
    (1000021, 1000011),
    (1000006, 1000071),
    (1000015, 1000101),
    (1000013, 1000041),
    (1000001, 1000056),
    (1000029, 1000049),
    (1000010, 1000075),
    (1000013, 1000047),
    (1000018, 1000018),
    (1000011, 1000081),
    (1000020, 1000068),
    (1000019, 1000002),
    (1000017, 1000020),
    (1000025, 1000008),
    (1000020, 1000062),
    (1000008, 1000036),
    (1000016, 1000014),
    (1000029, 1000052),
    (1000003, 1000031),
    (1000001, 1000054),
    (1000003, 1000032),
    (1000026, 1000099),
    (1000024, 1000013),
    (1000020, 1000066),
    (1000014, 1000029),
    (1000019, 1000001),
    (1000015, 1000104),
    (1000009, 1000094),
    (1000009, 1000092),
    (1000006, 1000069),
    (1000025, 1000007),
    (1000020, 1000067),
    (1000007, 1000024),
    (1000021, 1000009),
    (1000005, 1000060),
    (1000008, 1000035),
    (1000015, 1000102),
    (1000008, 1000038),
    (1000015, 1000100),
    (1000014, 1000028),
    (1000009, 1000093),
    (1000004, 1000023),
    (1000004, 1000022),
    (1000014, 1000027),
    (1000028, 1000084),
    (1000008, 1000040),
    (1000013, 1000044),
    (1000013, 1000043),
    (1000025, 1000006),
    (1000020, 1000064),
    (1000015, 1000103),
    (1000002, 1000076),
    (1000020, 1000061),
    (1000027, 1000077),
    (1000028, 1000090),
    (1000008, 1000037),
    (1000026, 1000097),
    (1000020, 1000065),
    (1000022, 1000057),
    (1000018, 1000019),
    (1000023, 1000016),
    (1000021, 1000010),
    (1000029, 1000051),
    (1000028, 1000082),
    (1000013, 1000048),
    (1000028, 1000086),
    (1000002, 1000075),
    (1000011, 1000078),
    (1000020, 1000063),
    (1000014, 1000025),
    (1000006, 1000073),
    (1000024, 1000012),
    (1000019, 1000003),
    (1000013, 1000045),
    (1000012, 1000083),
    (1000006, 1000074),
    (1000028, 1000087),
    (1000013, 1000046),
    (1000014, 1000030),
    (1000015, 1000105),
    (1000025, 1000005),
    (1000028, 1000089),
    (1000028, 1000091),
    (1000019, 1000004),
    (1000010, 1000076),
    (1000029, 1000050),
    (1000014, 1000026),
    (1000011, 1000079),
    (1000028, 1000085),
    (1000003, 1000034),
    (1000023, 1000017),
    (1000011, 1000080),
    (1000001, 1000055),
    (1000026, 1000096),
    (1000008, 1000039),
    (1000016, 1000015),
    (1000017, 1000021),
    (1000006, 1000070),
    (1000026, 1000098)
;
