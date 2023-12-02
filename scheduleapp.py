from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from observing.parsesdf import sdf_to_dict
from mnc import settings
import os
import fnmatch
import getpass
import time


app = FastAPI()
image_dir = "/home/claw/code/lwa-observing/images"
app.mount("/static", StaticFiles(directory=image_dir), name="static")
templates = Jinja2Templates(directory="templates")


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


class Settings(BaseModel):   # TODO: load this from mnc.settings load function
    time_loaded: str
    user: str
    filename: str
    time_file: str


class Calibration(BaseModel):  # TODO: load this from mnc xengine cal load function
    time_loaded: str
    filename: str
    beam: str


@app.on_event("startup")
async def startup_event():
    """Create database on startup."""
    create_db()


@app.get("/sessions", response_class=HTMLResponse)
async def get_all_sessions(request: Request):
    rows = read_sessions()
    sessions = [dict(PI_ID=row[0], PI_NAME=row[1], PROJECT_ID=row[2], SESSION_ID=row[3], SESSION_MODE=row[4], SESSION_DRX_BEAM=row[5], CONFIG_FILE=row[6], CAL_DIR=row[7], STATUS=row[8]) for row in rows]
    return templates.TemplateResponse("sessions.html", {"request": request, "sessions": sessions})


@app.get("/settings", response_class=HTMLResponse)
async def get_settings(request: Request):
    rows = read_settings()
    settings = [Settings(time_loaded=row[0], user=row[1], filename=row[2], time_file=row[3]) for row in rows]
    return templates.TemplateResponse("settings.html", {"request": request, "settings": settings})


@app.get("/calibrations", response_class=HTMLResponse)
async def get_calibrations(request: Request):
    rows = read_calibrations()
    calibrations = [dict(time_loaded=row[0], filename=row[1], beam=row[2]) for row in rows]
    return templates.TemplateResponse("calibrations.html", {"request": request, "calibrations": calibrations})


@app.get("/images", response_class=HTMLResponse)
async def get_images(request: Request):
    images = [f for f in os.listdir(image_dir) if fnmatch.fnmatch(f, '*.png')]
    return templates.TemplateResponse("images.html", {"request": request, "images": images})


def create_db():
    """Create database if it doesn't exist."""
    conn = sqlite3.connect('ovrolwa.db')
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
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("SELECT * FROM sessions")
    rows = c.fetchall()
    conn.close()
    return rows


def read_settings():
    """Read all settings from the database"""
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("SELECT * FROM settings")
    rows = c.fetchall()
    conn.close()
    return rows


def read_calibrations():
    """Read all calibrations from the database"""
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("SELECT * FROM calibrations")
    rows = c.fetchall()
    conn.close()
    return rows


def add_session(sdffile: str):
    """Parse SDF to create and add new session to the database."""
    assert os.path.exists(sdffile), f"{sdffile} does not exist"
    dd = sdf_to_dict(sdffile)
    # convert lists to comma-separated strings
    for key, value in dd['SESSION'].items():
        if isinstance(value, list):
            dd['SESSION'][key] = ', '.join(map(str, value))
    session = Session(**dd['SESSION'], STATUS='scheduled')
    conn = sqlite3.connect('ovrolwa.db')
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
    ss = settings.Settings(filename)

    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("INSERT INTO settings VALUES (?, ?, ?, ?)",
              (t_now, user, os.path.basename(ss.filename), ss.config['time']))
    conn.commit()
    conn.close()


def add_calibrations(filename, beam):
    """Add a new calibration to the calibrations table."""
    time_loaded = time.asctime(time.gmtime(time.time()))
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("INSERT INTO calibrations (time_loaded, filename, beam) VALUES (?, ?, ?)", (time_loaded, filename, beam))
    conn.commit()
    conn.close()


def read_latest_setting():
    """Read the most recent setting from the database."""
    conn = sqlite3.connect('ovrolwa.db')
    conn.row_factory = sqlite3.Row  # This enables column access by name: row['column_name'] 
    c = conn.cursor()
    c.execute("SELECT * FROM settings ORDER BY time_loaded DESC LIMIT 1")
    row = c.fetchone()
    conn.close()
    # Convert row to a dictionary if it's not None
    return dict(row) if row else None


def update_session(session_id, status):
    """ Update a status of a session in the database. """
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("UPDATE sessions SET status = ? WHERE SESSION_ID = ?", (status, session_id))
    conn.commit()
    conn.close()


def reset_table(table):
    """Reset the sessions table."""
    conn = sqlite3.connect('ovrolwa.db')
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


@app.get("/combined", response_class=HTMLResponse)
async def get_combined(request: Request):
    # Fetch data from the calibrations table
    calibrations_rows = read_calibrations()
    calibrations = [dict(time_loaded=row[0], filename=row[1]) for row in calibrations_rows]

    # Fetch data from the settings table
    settings_rows = read_settings()
    settings = [dict(time_loaded=row[0], user=row[1], filename=row[2], time_file=row[3]) for row in settings_rows]

    # Fetch data from the sessions table
    sessions_rows = read_sessions()
    sessions = [dict(PI_ID=row[0], PI_NAME=row[1], PROJECT_ID=row[2], SESSION_ID=row[3], SESSION_MODE=row[4], SESSION_DRX_BEAM=row[5], CONFIG_FILE=row[6], CAL_DIR=row[7], STATUS=row[8]) for row in sessions_rows]

    # Render the data into three tables
    return templates.TemplateResponse("combined.html", {"request": request, "calibrations": calibrations, "settings": settings, "sessions": sessions})