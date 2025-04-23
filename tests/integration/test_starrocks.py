"""
# Note: These tests are designed to validate the StarRocks testcontainers deployment.
# It will be replaced eventually with a more comprehensive test suite.
"""


def test_starrocks_iceberg_state(starrocks_iceberg_catalog, starrocks_session):
    with starrocks_session.cursor() as cursor:
        cursor.execute("SHOW CATALOGS")
        catalogs = cursor.fetchall()
        assert len(catalogs) == 2


def test_starrocks_create_table(starrocks_session):
    with starrocks_session.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS test_db")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_db.mock_table (
            id INT NOT NULL,
            name STRING NOT NULL,
            age INT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
        """)
        print("Table created successfully.")

        # Insert mock data
        insert_data_query = """
                INSERT INTO test_db.mock_table (id, name, age) VALUES
                (1, 'Alice', 30),
                (2, 'Bob', 25),
                (3, 'Charlie', 35);
                """
        cursor.execute(insert_data_query)
        starrocks_session.commit()
        print("Mock data inserted successfully.")

        # Query the table
        select_query = "SELECT * FROM test_db.mock_table;"
        cursor.execute(select_query)
        results = cursor.fetchall()
        assert len(results) == 3
