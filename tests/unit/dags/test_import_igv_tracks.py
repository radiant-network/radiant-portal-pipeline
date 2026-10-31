import pytest

from radiant.dags import NAMESPACE
from radiant.tasks.open_data.igv_tracks import IGV_TRACK_SOURCES

_DAG_ID = f"{NAMESPACE}-import-igv-tracks"


def test_dag_is_importable(dag_bag):
    assert _DAG_ID in dag_bag.dags
    assert dag_bag.import_errors == {}


def test_dag_maps_one_build_task_per_track(dag_bag):
    dag = dag_bag.get_dag(_DAG_ID)
    assert dag.schedule is None
    assert dag.max_active_runs == 1
    assert {t.task_id for t in dag.tasks} == {
        "list_igv_tracks",
        "build_igv_track_k8s",  # IS_AWS is unset in tests, so the k8s operator is built
    }
    build = dag.get_task("build_igv_track_k8s")
    assert build.upstream_task_ids == {"list_igv_tracks"}
    assert build.partial_kwargs["retries"] == 2
    assert build.partial_kwargs["max_active_tis_per_dagrun"] == 4
    assert build.partial_kwargs["map_index_template"] == "{{ task.op_kwargs['track_id'] }}"


def test_params(dag_bag):
    prefix = dag_bag.get_dag(_DAG_ID).params.get_param("igv_tracks_prefix")
    assert prefix.schema["type"] == "string"


@pytest.mark.parametrize("prefix", ["s3://tracks", "s3://tracks/", "s3://my-tracks.qa/igv/v_1/"])
def test_list_igv_tracks_accepts_plain_s3_prefixes(dag_bag, prefix):
    list_tracks = dag_bag.get_dag(_DAG_ID).get_task("list_igv_tracks").python_callable
    assert list_tracks(params={"igv_tracks_prefix": prefix})[0]["igv_tracks_prefix"] == prefix


def test_on_aws_the_dag_maps_the_ecs_task_over_the_listed_tracks(monkeypatch):
    """IS_AWS is read at import, so load a separate copy of the module with it set."""
    import importlib.util

    import radiant.dags
    from radiant.dags import import_igv_tracks

    monkeypatch.setattr(radiant.dags, "IS_AWS", True)
    # Names the operator container; the ECS operators refuse to build without it.
    monkeypatch.setenv("RADIANT_ENVIRONMENT", "mock")
    monkeypatch.setattr(
        radiant.dags.Variable,
        "get",
        lambda key: {"AWS_ECS_SUBNETS": "s1", "AWS_ECS_SECURITY_GROUPS": "g1"}.get(key, "cluster"),
    )
    spec = importlib.util.spec_from_file_location("import_igv_tracks_aws", import_igv_tracks.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dag = module.dag
    assert {t.task_id for t in dag.tasks} == {"list_igv_tracks", "build_igv_track_ecs"}
    build = dag.get_task("build_igv_track_ecs")
    assert build.upstream_task_ids == {"list_igv_tracks"}
    assert build.partial_kwargs["cluster"] == "cluster"
    # Hardcoded in the DAG, not an env var: chainSelf peaks at ~6.6 GB of disk.
    assert build.partial_kwargs["overrides"]["ephemeralStorage"] == {"sizeInGiB": 30}
    assert build.partial_kwargs["max_active_tis_per_dagrun"] == 4
    assert build.expand_input.value["params"].operator.task_id == "list_igv_tracks"


def test_list_igv_tracks_fans_out_every_source_with_the_prefix(dag_bag):
    list_tracks = dag_bag.get_dag(_DAG_ID).get_task("list_igv_tracks").python_callable
    tracks = list_tracks(params={"igv_tracks_prefix": "s3://tracks/igv/"})
    assert [t["track_id"] for t in tracks] == list(IGV_TRACK_SOURCES)
    assert len(tracks) == 11
    assert {t["igv_tracks_prefix"] for t in tracks} == {"s3://tracks/igv/"}


@pytest.mark.parametrize(
    "prefix", ["", None, "/local/igv/", "tracks/igv/", "s3://b/igv/' ; rm -rf / '", "s3://b/i g v/", "s3://"]
)
def test_list_igv_tracks_refuses_to_run_without_an_s3_prefix(dag_bag, prefix):
    list_tracks = dag_bag.get_dag(_DAG_ID).get_task("list_igv_tracks").python_callable
    with pytest.raises(ValueError, match="igv_tracks_prefix"):
        list_tracks(params={"igv_tracks_prefix": prefix})
