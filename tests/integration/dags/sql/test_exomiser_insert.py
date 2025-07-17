import os
import uuid

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

def test_raw_exomiser_load(starrocks_session, init_starrocks_tables, host_internal_address, minio_container, sample_exomiser_tsv):
    """
    Test the loading of raw Exomiser data into StarRocks.
    """
    with open(os.path.join(_SQL_DIR, "radiant/staging_exomiser_load.sql"), "r") as f_in:
        query = f_in.read()

    # Jinja template rendering
    rendered_sql = jinja2.Template(query).render({"params": get_radiant_mapping() | {"broker_load_timeout": 7200}})

    db_name = starrocks_session.db.decode("utf-8")

    rendered_sql = rendered_sql.format(
        database_name=db_name,
        label=f"test_raw_exomiser_load_{str(uuid.uuid4().hex)}",
        temporary_partition_clause="",
        broker_configuration=f"""
            'aws.s3.region' = 'us-east-1',
            'aws.s3.endpoint' = '{host_internal_address}:{minio_container.api_port}',
            'aws.s3.enable_path_style_access' = 'true',
            'aws.s3.access_key' = 'foo',
            'aws.s3.secret_key' = 'bar'
        """
    )

    params = {
        "part": 1,
        "seq_id": 1,
        "tsv_filepath": f"s3://{sample_exomiser_tsv}",
    }

    with starrocks_session.cursor() as cursor:
        cursor.execute(rendered_sql, params)
        _ = cursor.fetchall()

        cursor.execute(f"SELECT * FROM {db_name}.raw_exomiser")
        results = cursor.fetchall()

        assert len(results) == 4
        assert results[3][27] is None
