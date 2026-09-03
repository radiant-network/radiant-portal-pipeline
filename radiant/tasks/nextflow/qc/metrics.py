"""Find each case's DRAGEN metrics directory on S3, and group cases by it.

The metrics are not documents in the clinical model. In the layouts seen so far they sit in
the same directory as *some* output of the alignment task -- the gVCF in one, the CRAM in
another -- so the candidates are the parent directories of every output document of the
member's current alignment, and the one that actually holds `<aliquot>.mapping_metrics.csv`
wins. Probing beats guessing: the pipeline matches metrics to samplesheet rows by the exact
first dot-token of the filename and *fails open*, so a wrong directory is a green run with an
empty report, found by a human days later.

`--dragen_metrics_dir` is one directory per Nextflow run (verified against
quality-control-pipeline v2.0.0: the value is interpolated straight into globs, a comma list
matches nothing, and nf-schema rejects a brace pattern at launch). Hence `group_by_dir`: one
launcher run per distinct directory. A case whose members resolve to different directories
may use their common ancestor, but only if listing it shows no duplicated sample -- the same
aliquot under two subdirectories attaches both files to that sample.
"""

import logging
from collections.abc import Callable

from radiant.tasks.nextflow.paths import S3_SCHEME, split_s3_uri, to_mount
from radiant.tasks.nextflow.qc.model import MetricsGroup, QcCase, QcMember
from radiant.tasks.nextflow.resolve import ExcludedCase

LOGGER = logging.getLogger(__name__)

# `s3://bucket/dir` -> the object names directly under `dir/` (no recursion).
DirLister = Callable[[str], set[str]]
# `s3://bucket/dir` -> every object name under `dir/`, at any depth (basenames only).
TreeLister = Callable[[str], list[str]]


def metrics_filenames(aliquot: str) -> set[str]:
    """Both spellings the pipeline recognises for the file every DRAGEN run produces."""
    return {f"{aliquot}.mapping_metrics.csv", f"{aliquot}.final.mapping_metrics.csv"}


def parent_dir(url: str) -> str:
    """`s3://b/a/c/x.cram` -> `s3://b/a/c`. A url already ending in `/` is its own directory."""
    if url.endswith("/"):
        return url.rstrip("/")
    if "/" not in url[len(S3_SCHEME) :]:
        return url
    return url.rsplit("/", 1)[0]


def candidate_dirs(member: QcMember) -> list[str]:
    urls = [u for u in [member.cram_url, member.crai_url, *member.document_urls] if u]
    return sorted({parent_dir(u) for u in urls})


def common_ancestor(dirs: list[str]) -> str | None:
    """The deepest directory containing every one of `dirs`, or None if they span buckets."""
    if not dirs:
        return None
    split = [d[len(S3_SCHEME) :].rstrip("/").split("/") for d in dirs]
    common: list[str] = []
    for parts in zip(*split, strict=False):
        if len(set(parts)) != 1:
            break
        common.append(parts[0])
    # At least the bucket must be shared, or it is not one filesystem.
    if not common:
        return None
    return S3_SCHEME + "/".join(common)


class S3Lister:
    """The two listings the probe needs, over boto3. Constructed lazily so the pure functions
    above stay importable without AWS credentials."""

    def __init__(self, client=None):
        self._client = client

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        return self._client

    def list_dir(self, uri: str) -> set[str]:
        bucket, prefix = split_s3_uri(uri)
        names = set()
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/"
        ):
            names.update(obj["Key"].rsplit("/", 1)[-1] for obj in page.get("Contents", []))
        return names

    def list_tree(self, uri: str) -> list[str]:
        bucket, prefix = split_s3_uri(uri)
        return [
            obj["Key"].rsplit("/", 1)[-1]
            for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
            for obj in page.get("Contents", [])
        ]


def locate_metrics(
    cases: list[QcCase],
    list_dir: DirLister,
    list_tree: TreeLister,
    inputs_root: str,
) -> tuple[list[QcCase], list[ExcludedCase]]:
    """Set `metrics_dir_s3` on every member and case that can be resolved; exclude the rest.

    `inputs_root` is the workspace inputs bucket: a directory outside it is not on the FSx
    mount, so the pipeline could not read it however right it looks.
    """
    cache: dict[str, set[str]] = {}

    def names_in(directory: str) -> set[str]:
        if directory not in cache:
            cache[directory] = list_dir(directory)
        return cache[directory]

    kept, excluded = [], []
    for case in cases:
        problem = _locate_case(case, names_in, list_tree, inputs_root)
        if problem:
            reason, detail = problem
            excluded.append(ExcludedCase(case_id=case.case_id, reason=reason, detail=detail))
            LOGGER.warning("case %d excluded (%s): %s", case.case_id, reason, detail)
            continue
        kept.append(case)
    return kept, excluded


def group_by_dir(cases: list[QcCase], run_tag: str) -> list[MetricsGroup]:
    """One group -- one launcher run -- per distinct metrics directory, in a stable order."""
    by_dir: dict[str, list[int]] = {}
    for case in cases:
        by_dir.setdefault(case.metrics_dir_s3, []).append(case.case_id)
    return [
        MetricsGroup(index=index, run_tag=f"{run_tag}-g{index}", metrics_dir_s3=directory, case_ids=sorted(ids))
        for index, (directory, ids) in enumerate(sorted(by_dir.items()))
    ]


def _on_workspace(directory: str, inputs_root: str) -> bool:
    try:
        to_mount(directory, inputs_root, "/")
    except ValueError:
        return False
    return True


def _locate_case(case: QcCase, names_in: DirLister, list_tree: TreeLister, inputs_root: str) -> tuple[str, str] | None:
    for member in case.members:
        hits = [d for d in candidate_dirs(member) if metrics_filenames(member.aliquot) & names_in(d)]
        if not hits:
            return "no_dragen_metrics", (
                f"case {case.case_id}, aliquot {member.aliquot}: no mapping_metrics.csv in "
                f"{candidate_dirs(member) or 'no candidate directory (no documents)'}"
            )
        if len(hits) > 1:
            return "ambiguous_dragen_metrics", f"case {case.case_id}, aliquot {member.aliquot}: found in {hits}"
        if not _on_workspace(hits[0], inputs_root):
            return "metrics_not_on_workspace", f"case {case.case_id}, aliquot {member.aliquot}: {hits[0]}"
        member.metrics_dir_s3 = hits[0]

    directories = sorted({m.metrics_dir_s3 for m in case.members})
    if len(directories) == 1:
        case.metrics_dir_s3 = directories[0]
        return None

    ancestor = common_ancestor(directories)
    if ancestor is None or not _on_workspace(ancestor, inputs_root):
        return "metrics_dir_split", f"case {case.case_id}: {directories} share no directory on the workspace"
    names = list_tree(ancestor)
    for member in case.members:
        count = sum(1 for n in names if n in metrics_filenames(member.aliquot))
        if count != 1:
            return "metrics_dir_split", (
                f"case {case.case_id}: {ancestor} holds {count} mapping_metrics.csv for aliquot "
                f"{member.aliquot}; the pipeline needs exactly one"
            )
    case.metrics_dir_s3 = ancestor
    return None
