import pytest
from observing.obsstate import create_db

def test_create_db():
    # Test that the tables are created successfully
    create_db()

    # Check if the tables exist in the database
    with connection_factory() as conn:
        c = conn.cursor()

        # Check if 'sessions' table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'")
        result = c.fetchone()
        assert result is not None

        # Check if 'settings' table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='settings'")
        result = c.fetchone()
        assert result is not None

        # Check if 'calibrations' table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='calibrations'")
        result = c.fetchone()
        assert result is not None

        # Check if 'pis' table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pis'")
        result = c.fetchone()
        assert result is not None