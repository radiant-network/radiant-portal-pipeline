"""Case-driven inputs and outputs for the Ferlab quality-control-pipeline.

Sibling of the post-processing modules one level up, and built the same way: the DAG in
`radiant/dags/nextflow_quality_control_cases.py` supplies the I/O, these modules supply the
rules. `paths` and `portal` are shared with post-processing; everything pipeline-shaped --
the samplesheet, the output layout, the task type -- lives here.

Design: `design/SJRA-1879-nextflow-quality-control-automation.md`.
"""
