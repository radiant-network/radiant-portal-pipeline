from radiant.tasks.vcf.common import Common, process_common
from tests.unit.vcf.vcf_test_utils import variant


def test_common():
    v = variant("test_common.vcf")
    common = process_common(v, 1, 10)
    assert common == Common(
        1,
        10,
        "1-1000-A-C",
        "0b7ee091623299d5fbc548cfa374db94c84ae2e9682f27db1050e863502f0309",
        "1",
        1000,
        1000,
        "A",
        "C",
    )


def test_common_without_end():
    v = variant("test_common_without_end.vcf")
    common = process_common(v, 1, 10)
    assert common == Common(
        1,
        10,
        "1-1000-A-C",
        "0b7ee091623299d5fbc548cfa374db94c84ae2e9682f27db1050e863502f0309",
        "1",
        1000,
        1000,
        "A",
        "C",
    )
