import os
from unittest.mock import patch

from radiant.dags.operators.k8s import IgvTracks, _igv_tracks_container_resources


def test_igv_tracks_profile_requests_scratch_disk():
    """chainSelf spills ~5 GB of GFF3 through GNU sort, so the task asks for ephemeral storage."""
    with patch.dict(os.environ, {}, clear=True):
        resources = _igv_tracks_container_resources()
    assert resources.requests == {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "16Gi"}
    assert resources.limits == {"memory": "2Gi", "ephemeral-storage": "16Gi"}

    with patch.dict(os.environ, {"RADIANT_TASK_OPERATOR_IGV_TRACKS_EPHEMERAL_STORAGE": "30Gi"}, clear=True):
        assert _igv_tracks_container_resources().requests["ephemeral-storage"] == "30Gi"


def test_get_build_track():
    fake_env = {
        "RADIANT_TASK_OPERATOR_KUBERNETES_NAMESPACE": "airflow",
        "RADIANT_TASK_OPERATOR_SERVICE_ACCOUNT_NAME": "airflow-sa",
        "RADIANT_TASK_OPERATOR_IMAGE": "my-registry/radiant-task-operator:test",
    }
    with patch.dict(os.environ, fake_env, clear=True):
        op = IgvTracks.get_build_track(max_active_tracks=3)(
            track_id="clinvar", igv_tracks_prefix="s3://tracks/igv/"
        ).operator

    assert op.task_id == "build_igv_track_k8s"
    assert op.image == "my-registry/radiant-task-operator:test"
    assert op.do_xcom_push is True
    assert op.retries == 2
    assert op.max_active_tis_per_dagrun == 3
    assert op.container_resources.requests["ephemeral-storage"] == "16Gi"
    assert op.op_kwargs == {"track_id": "clinvar", "igv_tracks_prefix": "s3://tracks/igv/"}
    # No Iceberg namespace: the task never touches Iceberg.
    assert {e.name: e.value for e in op.env_vars}.get("RADIANT_ICEBERG_NAMESPACE") is None
