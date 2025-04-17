import os
from pathlib import Path

from cyvcf2 import VCF

# Base path of the current file
CURRENT_DIR = Path(__file__).parent

# Path to the resources directory
RESOURCES_DIR = CURRENT_DIR.parent.parent / "resources"

from contextlib import contextmanager


@contextmanager
def vcf(vcf_filename):
    p = os.path.join(RESOURCES_DIR, vcf_filename)
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
