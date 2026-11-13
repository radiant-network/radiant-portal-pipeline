"""Case-driven inputs and outputs for the Ferlab cnv-post-processing pipeline.

Sibling of `germline_snv/` and `qc/`, built the same way: the DAG in
`radiant/dags/nextflow_cnv_postprocessing_cases.py` supplies the I/O, these modules supply the
rules. `paths`, `portal` and `register` are shared; the PED and phenopacket writers of `family`
are reused through `CnvFamily` subclassing `Family`; everything pipeline-shaped --
the samplesheet, the output layout, the task types -- lives here.

Design: `design/cnv-post-processing-nextflow-integration.md`.
"""
