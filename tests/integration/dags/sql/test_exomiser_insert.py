

def test_sequencing_experiment_empty(starrocks_session, sample_exomiser_tsv):
    """
    Test the case where we "start from scratch" with nothing in the sequencing_experiment table.
    """



    with starrocks_session.cursor() as cursor:
        cursor.execute("SELECT * FROM staging_sequencing_experiment_delta;")
        results = cursor.fetchall()

    assert results is not None, "Results should not be None"
    result_df = pd.DataFrame(results, columns=sequencing_delta_columns)
    assert len(result_df) == 57