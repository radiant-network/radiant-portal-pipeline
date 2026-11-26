"""`vcf_utils.sample_phased`: a haploid sample is never phased, whatever cyvcf2's `gt_phases` reads."""

import pytest
from cyvcf2 import VCF

from radiant.tasks.vcf.vcf_utils import sample_phased

# Record 1: haploid record (A is the last sample, where gt_phases reads past the genotype array).
# Record 2: mixed ploidy (A haploid, padded; B phased; C unphased).
# Record 3: diploid, first allele missing.
VCF_TEXT = (
    "##fileformat=VCFv4.2\n"
    "##contig=<ID=chrX>\n"
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tA\tB\tC\n"
    "chrX\t1\t.\tA\tT\t.\tPASS\t.\tGT\t1\t1\t1\n"
    "chrX\t2\t.\tA\tT\t.\tPASS\t.\tGT\t1\t0|1\t0/1\n"
    "chrX\t3\t.\tA\tT\t.\tPASS\t.\tGT\t.|1\t1|1\t1/1\n"
)


@pytest.fixture(scope="module")
def records(tmp_path_factory):
    path = tmp_path_factory.mktemp("vcf_utils") / "phased.vcf"
    path.write_text(VCF_TEXT)
    return list(VCF(str(path)))


@pytest.mark.parametrize(
    ("record_idx", "expected"),
    [(0, [False, False, False]), (1, [False, True, False]), (2, [False, True, False])],
    ids=["haploid_record", "mixed_ploidy", "missing_first_allele"],
)
def test_sample_phased(records, record_idx, expected):
    record = records[record_idx]
    assert [sample_phased(record, i) for i in range(3)] == expected
