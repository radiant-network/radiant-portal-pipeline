"""clinvar_rcv_summary_insert.sql rewrites the RCV export's clinical significance
("Likely pathogenic, low penetrance") to the values of clinvar.interpretations (ClinVar VCF CLNSIG), so
both tables share dict_clinvar_interpretation.

The tables are test-local copies (the init DDL rendered under other names), so the shared clinvar
tables stay untouched.
"""

import json
import os
import uuid

import jinja2

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql", "open_data")


def test_clinvar_rcv_summary_insert_normalizes_clinical_significance(starrocks_session, radiant_mapping):
    suffix = uuid.uuid4().hex[:8]
    database_name = radiant_mapping["starrocks_clinvar_rcv_summary"].split(".")[0]
    mapping = {
        **radiant_mapping,
        "starrocks_clinvar": f"{database_name}.test_clinvar_{suffix}",
        "starrocks_raw_clinvar_rcv_summary": f"{database_name}.test_raw_clinvar_rcv_summary_{suffix}",
        "starrocks_clinvar_rcv_summary": f"{database_name}.test_clinvar_rcv_summary_{suffix}",
    }
    clinvar = mapping["starrocks_clinvar"]
    raw_rcv = mapping["starrocks_raw_clinvar_rcv_summary"]
    rcv = mapping["starrocks_clinvar_rcv_summary"]

    def render(path):
        with open(os.path.join(_SQL_DIR, path)) as f_in:
            return jinja2.Template(f_in.read()).render({"mapping": mapping})

    with starrocks_session.cursor() as cursor:
        try:
            cursor.execute(render("init/clinvar_create_table.sql"))
            cursor.execute(render("init/raw_clinvar_rcv_summary_create_table.sql"))
            cursor.execute(render("init/clinvar_rcv_summary_create_table.sql"))
            cursor.execute(f"INSERT INTO {clinvar} (locus_id, chromosome, name) VALUES (1, '1', '1'), (2, '1', '2')")
            cursor.execute(
                f"""
                INSERT INTO {raw_rcv} (clinvar_id, accession, clinical_significance)
                VALUES ('1', 'RCV1', ['Likely pathogenic, low penetrance', 'Pathogenic, low penetrance',
                                      'Likely benign']),
                       ('2', 'RCV2', ['no classifications from unflagged records',
                                      'Conflicting classifications of pathogenicity', 'Pathogenic'])
                """
            )

            cursor.execute(render("clinvar_rcv_summary_insert.sql"))

            cursor.execute(f"SELECT locus_id, clinical_significance FROM {rcv} ORDER BY locus_id")
            rows = {locus_id: json.loads(significance) for locus_id, significance in cursor.fetchall()}
            assert rows == {
                1: ["Likely_pathogenic_low_penetrance", "Pathogenic_low_penetrance", "Likely_benign"],
                2: [
                    "no_classifications_from_unflagged_records",
                    "Conflicting_classifications_of_pathogenicity",
                    "Pathogenic",
                ],
            }
        finally:
            for table in (clinvar, raw_rcv, rcv):
                cursor.execute(f"DROP TABLE IF EXISTS {table} FORCE")
