import pytest
from observing.obsstate import create_db, connection_factory, add_calibrations


def test_create_db():
    # Test that the tables are created successfully
    create_db('./ovrolwa_test.db')

    now = Time.now().mjd
    with connection_factory('./ovrolwa_test.db') as conn:
        c = conn.cursor()
        c.execute("INSERT INTO calibrations (time_loaded, filename, beam) VALUES (?, ?, ?)", (float(now), str(filename), str(beam)))


    # Check if the tables exist in the database
    with connection_factory('./ovrolwa_test.db') as conn:
        c = conn.cursor()

        # Check if 'sessions' table exists
        c.execute("SELECT * FROM calibrations")
        result = c.fetchone()
        assert result is not None
