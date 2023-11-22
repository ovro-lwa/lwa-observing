from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(directory="templates")


class Session(BaseModel):
    session_id: str   # unique. maybe id-mode-beam name instead? id could be mjd day number for vis or random int for beam
    session_mode: str
    session_beam: str
    pi_id: str
    pi_name: str
    config_file: str
    cal_dir: str
    settings_file: str


class ObservationBase(BaseModel):
    filename: str   # one file for beam, or root name for vis
    session_id: str   # should be in Session class
    obs_start: str
    obs_dur: str  # could be 10s for vis


class ObservationBeam(ObservationBase):
    obs_mode: str
    obs_target: str
    obs_ra: str
    obs_dec: str


@app.on_event("startup")
async def startup_event():
    """Create database on startup."""
    create_db()


@app.get("/sessions", response_class=HTMLResponse)
async def get_all_sessions(request: Request):
    rows = read_sessions()
    sessions = [dict(pi_id=row[0], session_mode=row[1], session_id=row[2], obs_target=row[3], obs_start=row[4]) for row in rows]
    return templates.TemplateResponse("landing_page.html", {"request": request, "sessions": sessions})


def create_db():
    """Create database if it doesn't exist."""
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS sessions
        (pi_id text, session_mode text, session_id text, obs_target text, obs_start text)
    ''')
    conn.commit()
    conn.close()


def read_sessions():
    """Read all sessions from the database via the sessions endpoint."""
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute("SELECT * FROM sessions")
    rows = c.fetchall()
    conn.close()
    return rows


def add_session(session: Session):
    """Add a new session to the database."""
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute("INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
              (session.pi_id, session.session_mode, session.session_id, session.obs_target, session.obs_start))
    conn.commit()
    conn.close()
