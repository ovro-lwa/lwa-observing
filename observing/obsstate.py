import os
import getpass
import time
from pydantic import BaseModel
import sqlite3
from observing import parsesdf
import logging

logger = logging.getLogger(__name__)

# TODO: figure out how to make it r/w for all users
#DBPATH = 'file:/opt/devel/pipeline/ovrolwa.db?mode=rw'
DBPATH = '/home/pipeline/proj/lwa-shell/lwa-observing/ovrolwa.db'

class Session(BaseModel):
    PI_ID: str
    PI_NAME: str
    PROJECT_ID: str
    SESSION_ID: str 
    SESSION_MODE: str
    SESSION_DRX_BEAM: str
    CONFIG_FILE: str
    CAL_DIR: str
    STATUS: str  # e.g., "scheduled" "completed"


class Settings(BaseModel):
    time_loaded: str
    user: str
    filename: str
    time_file: str


class Calibrations(BaseModel):
    time_loaded: str
    filename: str
    beam: str


class Product(BaseModel):
    time_loaded: str
    filename: str
    beam: str


def connection_factory():
    """Create a connection to the database."""
    return sqlite3.connect(DBPATH)


def create_db():
    """Create database if it doesn't exist."""
    with connection_factory() as conn:
        c = conn.cursor()
        c.executescript('''
            CREATE TABLE IF NOT EXISTS sessions
            (PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID text, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text);
            
            CREATE TABLE IF NOT EXISTS settings
            (time_loaded text, user text, filename text, time_file text);
            
            CREATE TABLE IF NOT EXISTS calibrations
            (time_loaded text, filename text, beam text);
        ''')


def read_sessions():
    """Read all sessions from the database"""
    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM sessions")
        rows = c.fetchall()

    return rows


def read_settings():
    """Read all settings from the database"""
    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM settings")
        rows = c.fetchall()

    return rows


def read_calibrations():
    """Read all calibrations from the database"""
    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM calibrations")
        rows = c.fetchall()

    return rows


def add_session(sdffile: str):
    """Parse SDF to create and add new session to the database."""
    assert os.path.exists(sdffile), f"{sdffile} does not exist"
    dd = parsesdf.sdf_to_dict(sdffile)
    # convert lists to comma-separated strings
    for key, value in dd['SESSION'].items():
        if isinstance(value, list):
            dd['SESSION'][key] = ', '.join(map(str, value))
    session = Session(**dd['SESSION'], STATUS='scheduled')

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (session.PI_ID, session.PI_NAME, session.PROJECT_ID, session.SESSION_ID, session.SESSION_MODE, 
                   session.SESSION_DRX_BEAM, session.CONFIG_FILE, session.CAL_DIR, session.STATUS))


def add_settings(filename: str, time_loaded: str):
    """Add settings to the database.

    Parameters
    ----------
    filename : str
        Name of the settings file.
    time_loaded : str
        Time the settings file was loaded.
    """

    assert os.path.exists(filename), f"{filename} does not exist"
    user = getpass.getuser()

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO settings VALUES (?, ?, ?, ?)",
                  (time_loaded, user, os.path.basename(filename), 0))   # TODO: figure out how to get time from file


def add_calibrations(filename, beam):
    """Add a new calibration to the calibrations table."""
    time_loaded = time.asctime(time.gmtime(time.time()))
    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO calibrations (time_loaded, filename, beam) VALUES (?, ?, ?)", (time_loaded, filename, beam))


def read_latest_setting():
    """Read the most recent setting from the database.
    Returns
    -------
    dict
        Dictionary containing the most recent settings.
    """

    with connection_factory() as conn:
        conn.row_factory = sqlite3.Row  # This enables column access by name: row['column_name'] 
        c = conn.cursor()
        c.execute("SELECT * FROM settings ORDER BY time_loaded DESC LIMIT 1")
        row = c.fetchone()

    # Convert row to a dictionary if it's not None
    return dict(row) if row else None


def update_session(session_id, status):
    """ Update a status of a session in the database. """
    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("UPDATE sessions SET status = ? WHERE SESSION_ID = ?", (status, session_id))


def reset_table(table):
    """Reset the sessions table."""
    with connection_factory() as conn:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS {table}")
        if table == 'sessions':
            c.execute('''
                CREATE TABLE sessions
                (PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID text, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text)
            ''')
        elif table == 'settings':
            c.execute('''
                CREATE TABLE settings
                (time_loaded text, user text, filename text, time_file text)
            ''')
        elif table == 'calibrations':
            c.execute('''
                CREATE TABLE calibrations
                (time_loaded text, filename text, beam text)
            ''')
        else:
            raise ValueError(f"{table} is not a valid table name")
