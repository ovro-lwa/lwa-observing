import os
import getpass
import time
from pydantic import BaseModel
import sqlite3
from observing import parsesdf
import logging

logger = logging.getLogger(__name__)

DBPATH = '/opt/devel/pipeline/ovrolwa.db'


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


def create_db():
    """Create database if it doesn't exist."""
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS sessions
        (PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID text, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS settings
        (time_loaded text, user text, filename text, time_file text)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS calibrations
        (time_loaded text, filename text, beam text)
    ''')
    conn.commit()
    conn.close()


def read_sessions():
    """Read all sessions from the database"""
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute("SELECT * FROM sessions")
    rows = c.fetchall()
    conn.close()
    return rows


def read_settings():
    """Read all settings from the database"""
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute("SELECT * FROM settings")
    rows = c.fetchall()
    conn.close()
    return rows


def read_calibrations():
    """Read all calibrations from the database"""
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute("SELECT * FROM calibrations")
    rows = c.fetchall()
    conn.close()
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
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
              (session.PI_ID, session.PI_NAME, session.PROJECT_ID, session.SESSION_ID, session.SESSION_MODE, 
               session.SESSION_DRX_BEAM, session.CONFIG_FILE, session.CAL_DIR, session.STATUS))
    conn.commit()
    conn.close()


def add_settings(filename: str):
    """Add settings to the database."""
    assert os.path.exists(filename), f"{filename} does not exist"
    user = getpass.getuser()
    t_now = time.asctime(time.gmtime(time.time()))

    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute("INSERT INTO settings VALUES (?, ?, ?, ?)",
              (t_now, user, os.path.basename(filename), 0))   # TODO: figure out how to get time from file
    conn.commit()
    conn.close()


def add_calibrations(filename, beam):
    """Add a new calibration to the calibrations table."""
    time_loaded = time.asctime(time.gmtime(time.time()))
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    try:
        c.execute("BEGIN")
        c.execute("INSERT INTO calibrations (time_loaded, filename, beam) VALUES (?, ?, ?)", (time_loaded, filename, beam))
        c.execute("COMMIT")
    except sqlite3.Error as e:
        print(f"An error occurred: {e.args[0]}")
        c.execute("ROLLBACK")
    finally:
        conn.close()

def read_latest_setting():
    """Read the most recent setting from the database."""
    conn = sqlite3.connect(DBPATH)
    conn.row_factory = sqlite3.Row  # This enables column access by name: row['column_name'] 
    c = conn.cursor()
    c.execute("SELECT * FROM settings ORDER BY time_loaded DESC LIMIT 1")
    row = c.fetchone()
    conn.close()
    # Convert row to a dictionary if it's not None
    return dict(row) if row else None


def update_session(session_id, status):
    """ Update a status of a session in the database. """
    conn = sqlite3.connect(DBPATH)
    c = conn.cursor()
    c.execute("UPDATE sessions SET status = ? WHERE SESSION_ID = ?", (status, session_id))
    conn.commit()
    conn.close()


def reset_table(table):
    """Reset the sessions table."""
    conn = sqlite3.connect(DBPATH)
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
    conn.commit()
    conn.close()


