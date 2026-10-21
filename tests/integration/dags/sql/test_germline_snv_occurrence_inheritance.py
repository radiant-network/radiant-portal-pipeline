"""RAD-94 -- the occurrence insert carries the slivar inheritance columns from Iceberg to StarRocks.

The insert is positional, so the four columns must land under their own names, after `comphet_genes`.
"""

import json
import os

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_PART = 9411
_TASK_ID = 94101
_TENANT = "test"
_PROBAND, _MOTHER = 9410101, 9410102
_SLIVAR_LOCUS, _ETL_LOCUS = 9411001, 9411002


def _occurrence(seq_id, locus_id, **inheritance):
    return {
        "task_id": _TASK_ID,
        "locus": f"1-{locus_id}-A-G",
        "locus_hash": f"hash-{locus_id}",
        "chromosome": "1",
        "start": locus_id,
        "reference": "A",
        "alternate": "G",
        "part": _PART,
        "seq_id": seq_id,
        "aliquot": f"SA{seq_id}",
        "tenant_code": _TENANT,
        "has_alt": True,
        "phased": False,
        **inheritance,
    }


_OCCURRENCES = [
    _occurrence(
        _PROBAND,
        _SLIVAR_LOCUS,
        parental_origin="MOTHER",
        transmission_mode="autosomal_recessive",
        other_transmission_modes=["autosomal_dominant"],
        transmission_mode_candidate=True,
        parental_origin_source="slivar",
        transmission_mode_source="slivar",
    ),
    _occurrence(
        _PROBAND,
        _ETL_LOCUS,
        parental_origin="DENOVO",
        transmission_mode="autosomal_dominant_de_novo",
        parental_origin_source="slivar",
        transmission_mode_source="etl",
    ),
    _occurrence(_MOTHER, _SLIVAR_LOCUS),
]


def _render(path, mapping, **context):
    with open(os.path.join(_SQL_DIR, path)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping, **context})


@pytest.fixture
def inserted(starrocks_session, iceberg_client, setup_iceberg_namespace, radiant_mapping):
    """Seed the Iceberg occurrences, run the occurrence insert, return the inheritance columns by sample/locus."""
    with starrocks_session.cursor() as cursor:
        for table in ("snv_tmp_variant", "exomiser", "germline_snv_comphet", "germline_snv_occurrence"):
            cursor.execute(_render(f"radiant/init/{table}_create_table.sql", radiant_mapping))
            cursor.execute(f"TRUNCATE TABLE {radiant_mapping[f'starrocks_{table}']}")
        cursor.executemany(
            f"INSERT INTO {radiant_mapping['starrocks_snv_tmp_variant']} (locus_id, locus_hash) VALUES (%s, %s)",
            [(locus_id, f"hash-{locus_id}") for locus_id in (_SLIVAR_LOCUS, _ETL_LOCUS)],
        )

    iceberg_table = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_occurrence")
    iceberg_table.append(pa.Table.from_pylist(_OCCURRENCES, schema=iceberg_table.schema().as_arrow()))

    target = radiant_mapping["starrocks_germline_snv_occurrence"]
    rendered_sql = _render(
        "radiant/germline_snv_occurrence_insert_partition_delta.sql", radiant_mapping, partition=_PART
    )
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping['iceberg_germline_snv_occurrence']}")
        # Positional INSERT INTO, the way `RadiantStarRocksPartitionSwapOperator` runs it.
        cursor.execute(
            f"INSERT INTO {target} {rendered_sql.rstrip().rstrip(';')}",
            {"task_ids": [_TASK_ID], "tenant_code": _TENANT},
        )
        cursor.execute(
            "SELECT seq_id, locus_id, parental_origin, transmission_mode, other_transmission_modes, "
            "transmission_mode_candidate, parental_origin_source, transmission_mode_source "
            f"FROM {target} WHERE part = {_PART}"
        )
        rows = {
            (seq_id, locus_id): (po, tm, json.loads(others) if others else None, candidate, po_source, tm_source)
            for seq_id, locus_id, po, tm, others, candidate, po_source, tm_source in cursor.fetchall()
        }

    yield rows
    iceberg_table.delete(EqualTo("task_id", _TASK_ID))


def test_inheritance_columns_land_in_starrocks(inserted):
    assert inserted == {
        (_PROBAND, _SLIVAR_LOCUS): (
            "MOTHER",
            "autosomal_recessive",
            ["autosomal_dominant"],
            1,
            "slivar",
            "slivar",
        ),
        (_PROBAND, _ETL_LOCUS): ("DENOVO", "autosomal_dominant_de_novo", None, None, "slivar", "etl"),
        # A parent gets no inheritance value.
        (_MOTHER, _SLIVAR_LOCUS): (None, None, None, None, None, None),
    }
