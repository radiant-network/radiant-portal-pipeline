import os

from cyvcf2 import VCF

HERE = os.path.dirname(__file__)
RESOURCES = os.path.join(HERE, "resources")
VCF_PATH = os.path.join(RESOURCES, "test_common.vcf")

from contextlib import contextmanager


@contextmanager
def vcf(vcf_filename):
    p = os.path.join(RESOURCES, vcf_filename)
    file = VCF(p, strict_gt=True)
    try:
        yield file
    finally:
        file.close()


def variant(vcf_filename, row_number=1):
    with vcf(vcf_filename) as vcf_file:
        for i, record in enumerate(vcf_file):
            if i == row_number - 1:
                return record
