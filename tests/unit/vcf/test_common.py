from tasks.vcf.common import Common, process_common
from tests.unit.vcf.vcf_test_utils import variant


def test_common():
    v = variant("test_common.vcf")
    common = process_common(v, 1)
    assert common == Common(1, "1-1000-A-C", "hash", "1", 1000, 1000, "A", "C")


def test_common_without_end():
    v = variant("test_common_without_end.vcf")
    common = process_common(v, 1)
    assert common == Common(1, "1-1000-A-C", "hash", "1", 1000, 1000, "A", "C")
