"""The FORMAT readers of `vcf_utils`: htslib's missing and padding values become None."""

import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from cyvcf2 import VCF

from radiant.tasks.vcf.cnv.germline.occurrence import SCHEMA as GERMLINE_CNV_SCHEMA
from radiant.tasks.vcf.cnv.somatic.occurrence import SCHEMA as SOMATIC_CNV_SCHEMA
from radiant.tasks.vcf.vcf_utils import format_array, format_value, format_values

# Record 1 has every field. B has `.` values, C a vector shorter than A's (padding). Record 2 leaves CN out.
VCF_TEXT = (
    "##fileformat=VCFv4.2\n"
    "##contig=<ID=chr1>\n"
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
    '##FORMAT=<ID=CN,Number=1,Type=Integer,Description="Copy number">\n'
    '##FORMAT=<ID=SM,Number=1,Type=Float,Description="Segment mean">\n'
    '##FORMAT=<ID=PE,Number=.,Type=Integer,Description="Paired-end reads">\n'
    '##FORMAT=<ID=AF,Number=.,Type=Float,Description="Allele fractions">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tA\tB\tC\n"
    "chr1\t1\tr1\tN\t<DEL>\t.\tPASS\t.\tGT:CN:SM:PE:AF\t0/1:3:0.5:5,7,9:0.25,0.5\t0/1:.:.:3:.,0.75\t0/1:2:1.5:.,4:.\n"
    "chr1\t10\tr2\tN\t<DEL>\t.\tPASS\t.\tGT:SM\t0/1:0.5\t0/1:0.5\t0/1:0.5\n"
)


@pytest.fixture(scope="module")
def records(tmp_path_factory):
    path = tmp_path_factory.mktemp("vcf_utils") / "format.vcf"
    path.write_text(VCF_TEXT)
    return list(VCF(str(path)))


@pytest.fixture(scope="module")
def full(records):
    return records[0]


@pytest.fixture(scope="module")
def without_cn(records):
    return records[1]


def test_format_array_is_none_for_a_field_the_header_does_not_declare(full):
    assert format_array(full, "XX") is None


def test_format_array_is_none_for_a_declared_field_the_record_leaves_out(without_cn):
    assert format_array(without_cn, "CN") is None


def test_format_array_gives_every_sample(full):
    assert len(format_array(full, "CN")) == 3


@pytest.mark.parametrize("field", ["XX", "CN"])
def test_absent_fields_read_as_none(without_cn, field):
    assert format_value(without_cn, field, 0, int) is None
    assert format_values(without_cn, field, 0, int) is None


@pytest.mark.parametrize("sample_idx,expected", [(0, 3), (1, None), (2, 2)])
def test_format_value_integer(full, sample_idx, expected):
    value = format_value(full, "CN", sample_idx, int)
    assert value == expected
    if expected is not None:
        assert type(value) is int


@pytest.mark.parametrize("sample_idx,expected", [(0, 0.5), (1, None), (2, 1.5)])
def test_format_value_float(full, sample_idx, expected):
    value = format_value(full, "SM", sample_idx, float)
    assert value == expected
    if expected is not None:
        assert type(value) is float


@pytest.mark.parametrize("sample_idx,expected", [(0, 5), (1, 3), (2, None)])
def test_format_value_reads_the_first_value_of_a_vector(full, sample_idx, expected):
    assert format_value(full, "PE", sample_idx, int) == expected


@pytest.mark.parametrize("sample_idx,expected", [(0, [5, 7, 9]), (1, [3]), (2, [None, 4])])
def test_format_values_integer_stops_at_the_padding_and_keeps_missing_values(full, sample_idx, expected):
    values = format_values(full, "PE", sample_idx, int)
    assert values == expected
    assert all(type(v) is int for v in values if v is not None)


@pytest.mark.parametrize("sample_idx,expected", [(0, [0.25, 0.5]), (1, [None, 0.75]), (2, None)])
def test_format_values_float(full, sample_idx, expected):
    assert format_values(full, "AF", sample_idx, float) == expected


@pytest.mark.parametrize("schema", [GERMLINE_CNV_SCHEMA, SOMATIC_CNV_SCHEMA], ids=["germline", "somatic"])
def test_a_partly_missing_pe_reaches_the_parquet_file(full, schema):
    pe_field = schema.as_arrow().field("pe")
    table = pa.Table.from_pylist([{"pe": format_values(full, "PE", 2, int)}], schema=pa.schema([pe_field]))
    pq.write_table(table, io.BytesIO())
