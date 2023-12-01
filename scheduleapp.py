from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from observing.parsesdf import sdf_to_dict
import os
import fnmatch


app = FastAPI()
app.mount("/static", StaticFiles(directory="/Users/claw/code/lwa-observing/images"), name="static")
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


@app.on_event("startup")
async def startup_event():
    """Create database on startup."""
    create_db()


@app.get("/sessions", response_class=HTMLResponse)
async def get_all_sessions(request: Request):
    rows = read_sessions()
    sessions = [dict(PI_ID=row[0], PI_NAME=row[1], PROJECT_ID=row[2], SESSION_ID=row[3], SESSION_MODE=row[4], SESSION_DRX_BEAM=row[5], CONFIG_FILE=row[6], CAL_DIR=row[7], STATUS=row[8]) for row in rows]
    return templates.TemplateResponse("landing_page.html", {"request": request, "sessions": sessions})


@app.get("/images", response_class=HTMLResponse)
async def get_images(request: Request):
    image_dir = "/Users/claw/code/lwa-observing/images"
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
        (time text, user text, filename text)
    ''')
    conn.commit()
    conn.close()


def read_sessions():
    """Read all sessions from the database via the sessions endpoint."""
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("SELECT * FROM sessions")
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


def update_session(session_id, status):
    """ Update a status of a session in the database. """
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("UPDATE sessions SET status = ? WHERE SESSION_ID = ?", (status, session_id))
    conn.commit()
    conn.close()


def reset_sessions_table():
    """Reset the sessions table."""
    conn = sqlite3.connect('ovrolwa.db')
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS sessions")
    c.execute('''
        CREATE TABLE sessions
        (PI_ID text, PI_NAME text, PROJECT_ID text, SESSION_ID text, SESSION_MODE text, SESSION_DRX_BEAM text, CONFIG_FILE text, CAL_DIR text, STATUS text)
    ''')
    conn.commit()
    conn.close()