import os
import getpass
import time
from astropy.time import Time
from pydantic import BaseModel
import sqlite3
from observing import parsesdf
import logging

logger = logging.getLogger(__name__)

# TODO: figure out how to make it r/w for all users
DBPATH = '/opt/devel/pipeline/ovrolwa.db'
#DBPATH = '/home/pipeline/proj/lwa-shell/lwa-observing/ovrolwa.db'

class Session(BaseModel):
    time_loaded: str
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
            (time_loaded text, PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID text, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text);
            
            CREATE TABLE IF NOT EXISTS settings
            (time_loaded text, user text, filename text);
            
            CREATE TABLE IF NOT EXISTS calibrations
            (time_loaded text, filename text, beam text);
        ''')


def read_sessions():
    """Read all sessions from the database"""

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM sessions ORDER BY time_loaded DESC")
        rows = c.fetchall()

    return rows


def read_settings():
    """Read all settings from the database"""

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM settings ORDER BY time_loaded DESC")
        rows = c.fetchall()

    return rows


def read_calibrations():
    """Read all calibrations from the database"""

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM calibrations ORDER BY time_loaded DESC")
        rows = c.fetchall()

    return rows


def add_session(sdffile: str):
    """Parse SDF to create and add new session to the database."""

    assert os.path.exists(sdffile), f"{sdffile} does not exist"
    dd = parsesdf.sdf_to_dict(sdffile)
    now = Time.now()
    # convert lists to comma-separated strings
    for key, value in dd['SESSION'].items():
        if isinstance(value, list):
            dd['SESSION'][key] = ', '.join(map(str, value))

    session = Session(**dd['SESSION'], time_loaded=now.mjd, STATUS='scheduled')

    with connection_factory() as conn:
        c = conn.cursor()

        # check whether session_id already exists in database
        c.execute("SELECT 1 FROM sessions WHERE session_id = ?", (session.SESSION_ID,))
        if c.fetchone() is not None:
            logger.warning(f"Session ID {session.SESSION_ID} already exists in the database. Skipping...")
            return

        c.execute("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (session.time_loaded, session.PI_ID, session.PI_NAME, session.PROJECT_ID, session.SESSION_ID,
                   session.SESSION_MODE, session.SESSION_DRX_BEAM, session.CONFIG_FILE, session.CAL_DIR,
                   session.STATUS))


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
        c.execute("INSERT INTO settings VALUES (?, ?, ?)",
                  (time_loaded, user, os.path.basename(filename)))


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


def iterate_max_session_id():
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute("SELECT MAX(session_id) FROM sessions")
    try:
        max_session_id = int(c.fetchone()[0])
    except TypeError:
        max_session_id = 0
    conn.close()
    return str(max_session_id+1)


def reset_table(table):
    """Reset the sessions table."""

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS {table}")
        if table == 'sessions':
            c.execute('''
                CREATE TABLE sessions
                (time_loaded text, PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID text, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text)
            ''')
        elif table == 'settings':
            c.execute('''
                CREATE TABLE settings
                (time_loaded text, user text, filename text)
            ''')
        elif table == 'calibrations':
            c.execute('''
                CREATE TABLE calibrations
                (time_loaded text, filename text, beam text)
            ''')
        else:
            raise ValueError(f"{table} is not a valid table name")
