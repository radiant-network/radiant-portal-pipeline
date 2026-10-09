"""Build the public IGV reference tracks (ClinVar, DGV, gnomAD SV/CNV, ClinGen dosage, UCSC repeats, ...).

A port of Qlin's ``etl_import_igv``. One mapped task per track (radiant-operator image, Kubernetes or ECS)
downloads the upstream file, converts it to GFF3 when needed, sorts, bgzips and tabix-indexes it, and writes
the file and its ``.tbi`` under ``igv_tracks_prefix``. The portal API presigns them for the IGV Tracks menu.
Files only: no StarRocks or Iceberg table. See `radiant/tasks/open_data/igv_tracks.py` and
docs/import_igv_tracks.md.
"""

import os
import re

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param

from radiant.dags import DEFAULT_ARGS, ICEBERG_NAMESPACE, IS_AWS, NAMESPACE, ECSEnv, load_docs_md
from radiant.tasks.open_data.igv_tracks import IGV_TRACK_SOURCES

if IS_AWS:
    from radiant.dags.operators import ecs as operators
else:
    from radiant.dags.operators import k8s as operators

IGV_TRACKS_PREFIX_ENV = "RADIANT_IGV_TRACKS_S3_PREFIX"
# Bucket + key characters only: on ECS the prefix is spliced into a single-quoted shell command.
IGV_TRACKS_PREFIX_RE = re.compile(r"^s3://[A-Za-z0-9.\-]+(/[A-Za-z0-9._\-/]*)?$")

dag_params = {
    "igv_tracks_prefix": Param(
        default=os.getenv(IGV_TRACKS_PREFIX_ENV, ""),
        type="string",
        title="IGV tracks prefix",
        description=(
            f"S3 prefix the tracks and their .tbi are written under (e.g. s3://<tracks bucket>/igv/). Defaults to "
            f"the {IGV_TRACKS_PREFIX_ENV} environment variable. A rerun replaces the files with the latest "
            f"upstream release."
        ),
    ),
}

with DAG(
    dag_id=f"{NAMESPACE}-import-igv-tracks",
    dag_display_name="Radiant - Import IGV Tracks",
    schedule=None,
    catchup=False,
    # Two runs would write the same objects: let one finish before the next starts.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=dag_params,
    render_template_as_native_obj=True,
    tags=["radiant", "open-data", "igv", "manual"],
    doc_md=load_docs_md("import_igv_tracks.md"),
) as dag:

    @task(task_id="list_igv_tracks", task_display_name="[PyOp] List IGV Tracks")
    def list_igv_tracks(params: dict | None = None) -> list[dict[str, str]]:
        prefix = (params or {}).get("igv_tracks_prefix") or ""
        if not IGV_TRACKS_PREFIX_RE.match(prefix):
            raise ValueError(
                f"igv_tracks_prefix must be an s3:// URI of letters, digits and . _ - / only, got {prefix!r}: "
                f"set the param or {IGV_TRACKS_PREFIX_ENV}"
            )
        return [{"track_id": track_id, "igv_tracks_prefix": prefix} for track_id in IGV_TRACK_SOURCES]

    tracks = list_igv_tracks()

    if IS_AWS:
        operators.IgvTracks.get_build_track(ecs_env=ECSEnv()).expand(params=tracks)
    else:
        # The Iceberg namespace is only part of the shared pod context; this task never touches Iceberg.
        operators.IgvTracks.get_build_track(radiant_namespace=ICEBERG_NAMESPACE).expand_kwargs(tracks)
