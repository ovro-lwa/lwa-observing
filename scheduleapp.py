from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(directory="templates")

class Session(BaseModel):
    pi_id: str
    session_mode: str
    session_id: str
    obs_target: str
    obs_start: str

def create_db():
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS sessions
        (pi_id text, session_mode text, session_id text, obs_target text, obs_start text)
    ''')
    conn.commit()
    conn.close()

@app.on_event("startup")
async def startup_event():
    create_db()

@app.post("/sessions/")
async def create_session(session: Session):
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute("INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
              (session.pi_id, session.session_mode, session.session_id, session.obs_target, session.obs_start))
    conn.commit()
    conn.close()
    return {"message": "Session created successfully"}

@app.get("/sessions/")
async def read_sessions():
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute("SELECT * FROM sessions")
    rows = c.fetchall()
    conn.close()
    return rows

@app.get("/sessions/{session_id}/update", response_class=HTMLResponse)
async def get_update_form(request: Request, session_id: str):
    return templates.TemplateResponse("update_form.html", {"request": request, "session_id": session_id})

@app.get("/sessions", response_class=HTMLResponse)
async def get_all_sessions(request: Request):
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute("SELECT * FROM sessions")
    rows = c.fetchall()
    conn.close()
    sessions = [dict(pi_id=row[0], session_mode=row[1], session_id=row[2], obs_target=row[3], obs_start=row[4]) for row in rows]
    return templates.TemplateResponse("landing_page.html", {"request": request, "sessions": sessions})

def add_session(session: Session):
    conn = sqlite3.connect('sessions.db')
    c = conn.cursor()
    c.execute("INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
              (session.pi_id, session.session_mode, session.session_id, session.obs_target, session.obs_start))
    conn.commit()
    conn.close()

