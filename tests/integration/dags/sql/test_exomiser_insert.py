import os
import time
import uuid

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_SQL_DIR = os.path.join(DAGS_DIR, "sql")


def test_raw_exomiser_load(starrocks_session, host_internal_address, minio_container, sample_exomiser_tsv):
    """
    Test the loading of raw Exomiser data into StarRocks.
    """
    with open(os.path.join(_SQL_DIR, "radiant/init/staging_exomiser_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"params": get_radiant_mapping()})

    with open(os.path.join(_SQL_DIR, "radiant/staging_exomiser_load.sql")) as f_in:
        query = f_in.read()

    # Jinja template rendering
    rendered_sql = jinja2.Template(query).render({"params": get_radiant_mapping() | {"broker_load_timeout": 7200}})

    db_name = starrocks_session.db.decode("utf-8")

    _label = f"test_raw_exomiser_load_{str(uuid.uuid4().hex)}"
    rendered_sql = rendered_sql.format(
        database_name=db_name,
        label=_label,
        temporary_partition_clause="",
        broker_configuration=f"""
            'aws.s3.region' = 'us-east-1',
            'aws.s3.endpoint' = 'http://{host_internal_address}:{minio_container.api_port}',
            'aws.s3.enable_path_style_access' = 'true',
            'aws.s3.access_key' = '{minio_container.access_key}',
            'aws.s3.secret_key' = '{minio_container.secret_key}'
        """,
    )

    params = {
        "part": 1,
        "seq_id": 1,
        "tsv_filepath": f"s3://{sample_exomiser_tsv}",
    }

    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        cursor.execute(rendered_sql, params)
        _ = cursor.fetchall()

        _i = 0
        while True:
            cursor.execute("SELECT STATE FROM information_schema.loads WHERE LABEL = %(label)s", {"label": _label})
            load_state = cursor.fetchone()
            if not load_state or load_state[0] == "FINISHED":
                break
            if load_state[0] == "CANCELLED":
                raise RuntimeError(f"Load for label {_label} was cancelled.")
            time.sleep(2)
            _i += 1
            if _i > 30:
                raise TimeoutError(f"Load for label {_label} did not finish in time.")

        cursor.execute(f"SELECT rank, acmg_classification, acmg_evidence FROM {db_name}.raw_exomiser ORDER BY rank")
        results = cursor.fetchall()
        assert results == (
            (1, "pathogenic", '["PM2","PP3","PS4"]'),
            (2, "likely_pathogenic", '["PS1","PM1"]'),
            (3, "uncertain_significance", '["PP5"]'),
            (4, "uncertain_significance", None),
        )
