"""S3 URIs, FSx pod paths, and the translation between them.

The shared workspace is an FSx-Lustre filesystem mounted at `/workspace` on the Nextflow
pods. Its `inputs` prefix is auto-imported from one S3 bucket and its `outputs` prefix
auto-exported to another, so an object key `foo/bar.gvcf.gz` in the inputs bucket appears
to the pipeline as `/workspace/inputs/foo/bar.gvcf.gz`.

That is why the Airflow tasks can write and list plain S3 while the samplesheet and the
`outdir` param carry pod paths: they are two views of the same bytes.
"""

from urllib.parse import urlparse

S3_SCHEME = "s3://"


def sanitize_run_tag(run_id: str) -> str:
    """The run tag, from `run_id`: `scheduled__2026-09-03T00:00:00+00:00` -> `scheduled-2026-09-03T00-00-00-00-00`.

    Not derived from a timestamp: an Airflow 3 manual run can have a null `logical_date`,
    and date-derived templates then raise at render time. `run_id` is also stable across
    task retries, so the paths stay put and `-resume` keeps working.

    The `__` after the run type is replaced too, and that is load-bearing: the cases DAGs pin
    the *child* run id to this tag, and Airflow 3.2 refuses to create an operator-triggered
    (manual) run whose id starts with `scheduled__` or another type's reserved prefix
    (`DagRunType.from_run_id` in `create_dagrun`). The API answers that with an opaque 500.
    """
    return run_id.replace(":", "-").replace("+", "-").replace("__", "-")


def split_s3_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/a/b` -> `("bucket", "a/b")`. The key has no leading or trailing slash."""
    if not uri.startswith(S3_SCHEME):
        raise ValueError(f"not an s3 uri: {uri!r}")
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.strip("/")


def join_s3(uri: str, *parts: str) -> str:
    return "/".join([uri.rstrip("/"), *[p.strip("/") for p in parts]])


def to_mount(url: str, s3_root: str, mount: str) -> str:
    """Map an S3 URL in the workspace bucket to the path the pipeline sees.

    The bucket check is a real safeguard, not defensive style: a gVCF registered in some
    other bucket is not on the FSx mount at all, and without this the run would fail hours
    later with a missing-file error from a Nextflow worker instead of here.
    """
    bucket, _ = split_s3_uri(s3_root)
    prefix = f"{S3_SCHEME}{bucket}/"
    if not url.startswith(prefix):
        raise ValueError(f"{url!r} is not in the workspace bucket {bucket!r}, so it is not on the shared filesystem")
    return f"{mount.rstrip('/')}/{url[len(prefix) :]}"


def run_paths(
    inputs_root: str,
    outputs_root: str,
    inputs_mount: str,
    outputs_mount: str,
    run_tag: str,
    *,
    inputs_subdir: str = "",
    outputs_subdir: str = "",
) -> dict:
    """Every path this run reads or writes, derived from the run tag alone.

    Deriving rather than parameterising makes "a fresh prefix per run" structural instead
    of a rule someone has to remember, and gives each run its own output location -- which
    matters because re-runs sit alongside earlier analyses rather than replacing them.

    `inputs_subdir` / `outputs_subdir` keep the pipelines apart under the shared roots:
    post-processing writes `postprocessing-runs/<run>` and `postprocessing/<run>`, quality
    control `qc-runs/<run>` and `qc/<run>`. Empty means directly under the root.
    """
    input_prefix_s3 = join_s3(inputs_root, *_parts(inputs_subdir), run_tag)
    _, inputs_root_key = split_s3_uri(inputs_root)
    _, outputs_root_key = split_s3_uri(outputs_root)

    def under(mount: str, root_key: str, subdir: str) -> str:
        return "/".join([mount.rstrip("/"), *([root_key] if root_key else []), *_parts(subdir), run_tag])

    input_prefix_pod = under(inputs_mount, inputs_root_key, inputs_subdir)
    return {
        "run_tag": run_tag,
        "input_prefix_s3": input_prefix_s3,
        "input_prefix_pod": input_prefix_pod,
        "samplesheet_s3": f"{input_prefix_s3}/samplesheet.csv",
        "samplesheet_pod": f"{input_prefix_pod}/samplesheet.csv",
        "outdir_s3": join_s3(outputs_root, *_parts(outputs_subdir), run_tag),
        "outdir_pod": under(outputs_mount, outputs_root_key, outputs_subdir),
    }


def _parts(subdir: str) -> list[str]:
    return [subdir.strip("/")] if subdir.strip("/") else []
