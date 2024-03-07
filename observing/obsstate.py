import os
import getpass
from astropy.time import Time
from pydantic import BaseModel
import sqlite3
from observing import parsesdf
from slack_sdk import WebClient
import logging

logger = logging.getLogger(__name__)
if "SLACK_TOKEN_LWA" in os.environ:
    cl = WebClient(token=os.environ["SLACK_TOKEN_LWA"])
else:
    cl = None
    logging.warning("No SLACK_TOKEN_LWA found. No slack updates.")

# TODO: figure out how to make it r/w for all users
DBPATH = '/opt/devel/pipeline/ovrolwa.db'
#DBPATH = '/home/pipeline/proj/lwa-shell/lwa-observing/ovrolwa.db'

class Session(BaseModel):
    time_loaded: float
    PI_ID: str
    PI_NAME: str
    PROJECT_ID: str
    SESSION_ID: int 
    SESSION_MODE: str
    SESSION_DRX_BEAM: str
    CONFIG_FILE: str
    CAL_DIR: str
    STATUS: str  # e.g., "scheduled" "completed"

class Settings(BaseModel):
    time_loaded: float
    user: str
    filename: str

class Calibrations(BaseModel):
    time_loaded: float
    filename: str
    beam: str

class Product(BaseModel):
    time_loaded: float
    filename: str
    beam: str

class PIs(BaseModel):
    PI_ID: int
    PI_NAME: str


def connection_factory(path=DBPATH):
    """Create a connection to the database."""
    return sqlite3.connect(path)


def create_db(path=DBPATH):
    """Create database if it doesn't exist."""

    with connection_factory(path=path) as conn:
        c = conn.cursor()
        c.executescript('''
            CREATE TABLE IF NOT EXISTS sessions
            (time_loaded float, PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID integer, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text);
            
            CREATE TABLE IF NOT EXISTS settings
            (time_loaded float, user text, filename text);
            
            CREATE TABLE IF NOT EXISTS calibrations
            (time_loaded float, filename text, beam text);
                        
            CREATE TABLE IF NOT EXISTS pis
            (PI_ID integer, PI_NAME text);
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


def read_pis():
    """Read all PIs from the database"""

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM pis ORDER BY PI_ID")
        rows = c.fetchall()

    return rows


def add_session(sdffile: str):
    """Parse SDF to create and add new session to the database."""

    assert os.path.exists(sdffile), f"{sdffile} does not exist"
    dd = parsesdf.sdf_to_dict(sdffile)
    now = Time.now().mjd

    # convert lists to comma-separated strings
    for key, value in dd['SESSION'].items():
        if isinstance(value, list):
            dd['SESSION'][key] = ', '.join(map(str, value))

    session = Session(**dd['SESSION'], time_loaded=float(now), STATUS='scheduled')

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

    if cl is not None:
        response = cl.chat_postMessage(channel="#observing",
                                       text=f"Session {session.SESSION_ID} submitted for {session.PI_NAME} for mode {session.SESSION_MODE}",
                                       icon_emoji = ":robot_face::")


def add_settings(filename: str):
    """Add settings to the database.

    Parameters
    ----------
    filename : str
        Name of the settings file.
    """

    assert os.path.exists(filename), f"{filename} does not exist"
    user = getpass.getuser()
    time_loaded = Time.now().mjd

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO settings VALUES (?, ?, ?)",
                  (time_loaded, str(user), os.path.basename(filename)))

    if cl is not None:
        response = cl.chat_postMessage(channel="#observing",
                                       text=f"Settings updated by {user} with file {filename}",
                                       icon_emoji = ":robot_face::")


def add_calibrations(filename, beam):
    """Add a new calibration to the calibrations table."""


    now = Time.now().mjd
    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO calibrations (time_loaded, filename, beam) VALUES (?, ?, ?)", (float(now), str(filename), str(beam)))


def add_pi(pi_id, pi_name):
    """Add a new PI to the pis table."""

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO pis VALUES (?, ?)", (pi_id, pi_name))


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
    """ Return the 1 plus the maximum session_id in the database.
    """

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT MAX(session_id) FROM sessions")
        try:
            max_session_id = int(c.fetchone()[0])
        except TypeError:
            max_session_id = 0

    return max_session_id+1


def check_and_create_pi(pi_name):
    """
    Check if a PI exists in the database, and if not, create one.
    """

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute("SELECT pi_id FROM pis WHERE pi_name = ?", (pi_name,))
        row = c.fetchone()
        if row:
            return row[0]
        else:
            c.execute("SELECT MAX(pi_id) FROM pis")
            max_pi_id0 = c.fetchone()
            if len(max_pi_id0):
                max_pi_id = int(max_pi_id0[0])
            else:
                max_pi_id = 0
            new_pi_id = max_pi_id + 1
            c.execute("INSERT INTO pis VALUES (?, ?)", (new_pi_id, pi_name))
        return new_pi_id


def reset_table(table):
    """Reset the sessions table."""

    with connection_factory() as conn:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS {table}")
        if table == 'sessions':
            c.execute('''
                CREATE TABLE sessions
                (time_loaded float, PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID integer, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text)
            ''')
        elif table == 'settings':
            c.execute('''
                CREATE TABLE settings
                (time_loaded float, user text, filename text)
            ''')
        elif table == 'calibrations':
            c.execute('''
                CREATE TABLE calibrations
                (time_loaded float, filename text, beam text)
            ''')
        elif table == 'pis':
            c.execute('''
                CREATE TABLE pis
                (PI_ID integer, PI_NAME text)
            ''')
        else:
            raise ValueError(f"{table} is not a valid table name")
