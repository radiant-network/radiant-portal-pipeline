"""What the Nextflow pipelines share when registering their outputs."""

# The Ferlab snv-post-processing pipeline, recorded on the germline and somatic SNV annotation
# tasks -- deliberately not the variant caller upstream of the run. The revision the launcher
# image pins (`PIPELINE_REV` in Dockerfile.nextflow.launcher): a commit of `main` until the next
# release is tagged. Bump both together.
ANNOTATION_PIPELINE = ("snv-post-processing", "0df1ac0")

# The spelling already stored in the tenant databases. The casing is odd on purpose:
# matching what is there matters more than being right about it here.
GENOME_BUILD = "GRch38"


class MissingOutputsError(Exception):
    """A pipeline run published less than a complete set for at least one family."""
