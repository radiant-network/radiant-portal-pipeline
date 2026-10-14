import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from radiant.dags.operators.ecs import (
    CosmicMutationSet,
    ImportPart,
    ImportSNVVCF,
    InitIcebergTables,
    RadiantTaskECSOperator,
)

ECS_ENV = SimpleNamespace(ECS_CLUSTER="my-cluster", ECS_SUBNETS=["subnet-1"], ECS_SECURITY_GROUPS=["sg-1"])

BUILDERS = {
    "create_germline_parquet_files": lambda: ImportSNVVCF.get_create_germline_parquet_files("ns", ECS_ENV),
    "create_somatic_parquet_files": lambda: ImportSNVVCF.get_create_somatic_parquet_files("ns", ECS_ENV),
    "commit_partitions": lambda: ImportSNVVCF.get_commit_partitions("ns", ECS_ENV),
    "init_iceberg": lambda: InitIcebergTables.get_init_iceberg("ns", "my_table", ECS_ENV),
    "import_cnv_vcf": lambda: ImportPart.get_import_cnv_vcf("ns", ECS_ENV),
    "import_somatic_cnv_vcf": lambda: ImportPart.get_import_somatic_cnv_vcf("ns", ECS_ENV),
    "cleanup": lambda: ImportPart.get_cleanup(ECS_ENV),
    "normalize_cosmic_mutation_set": lambda: CosmicMutationSet.get_normalize(ECS_ENV),
}


def _overrides(op):
    # `.partial(...)` builders return an OperatorPartial, which keeps its arguments in `kwargs`.
    if hasattr(op, "kwargs"):
        # Silence the "was never mapped" warning an unexpanded partial emits when collected.
        op._expand_called = True
        return op.kwargs["overrides"]
    return op.overrides


@pytest.mark.parametrize("builder", BUILDERS.values(), ids=BUILDERS.keys())
def test_container_name_follows_environment(builder):
    with patch.dict(os.environ, {"RADIANT_ENVIRONMENT": "mock"}, clear=True):
        op = builder()

    (container,) = _overrides(op)["containerOverrides"]
    assert container["name"] == "radiant-operator-mock-etl-container"


@pytest.mark.parametrize("value", [None, ""])
def test_container_name_requires_environment(value):
    env = {} if value is None else {"RADIANT_ENVIRONMENT": value}
    with patch.dict(os.environ, env, clear=True), pytest.raises(ValueError, match="RADIANT_ENVIRONMENT"):
        RadiantTaskECSOperator._container_name()
