from fastapi import FastAPI
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from observing import obsstate as obs
import os
import fnmatch
import logging

logging.basicConfig(level=logging.INFO)  # This configures the root logger
logger = logging.getLogger(__name__)

app = FastAPI()
image_dir = os.path.dirname(os.path.realpath(__file__))
app.mount("/static", StaticFiles(directory=image_dir), name="static")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
async def startup_event():
    """Create database on startup."""
    obs.create_db()


@app.get("/sessions", response_class=HTMLResponse)
async def get_all_sessions(request: Request):
    rows = obs.read_sessions()
    sessions = [obs.Session(PI_ID=row[0], PI_NAME=row[1], PROJECT_ID=row[2], SESSION_ID=row[3], SESSION_MODE=row[4], SESSION_DRX_BEAM=row[5], CONFIG_FILE=row[6], CAL_DIR=row[7], STATUS=row[8]) for row in rows]
    return templates.TemplateResponse("sessions.html", {"request": request, "sessions": sessions})


@app.get("/settings", response_class=HTMLResponse)
async def get_settings(request: Request):
    rows = obs.read_settings()
    settings = [obs.Settings(time_loaded=row[0], user=row[1], filename=row[2], time_file=row[3]) for row in rows]
    return templates.TemplateResponse("settings.html", {"request": request, "settings": settings})


@app.get("/calibrations", response_class=HTMLResponse)
async def get_calibrations(request: Request):
    rows = obs.read_calibrations()
    calibrations = [obs.Calibrations(time_loaded=row[0], filename=row[1], beam=row[2]) for row in rows]
    return templates.TemplateResponse("calibrations.html", {"request": request, "calibrations": calibrations})


@app.get("/images", response_class=HTMLResponse)
async def get_images(request: Request):
    images = [f for f in os.listdir(image_dir) if fnmatch.fnmatch(f, '*.png')]
    return templates.TemplateResponse("images.html", {"request": request, "images": images})


@app.get("/", response_class=HTMLResponse)
async def get_combined(request: Request):
    # Fetch data from the calibrations table
    calibrations = [obs.Calibrations(time_loaded=row[0], filename=row[1], beam=row[2]) 
                    for row in obs.read_calibrations()]

    # Fetch data from the settings table
    settings = [obs.Settings(time_loaded=row[0], user=row[1], filename=row[2], time_file=row[3]) 
                for row in obs.read_settings()]

    # Fetch data from the sessions table
    sessions = [obs.Session(PI_ID=row[0], PI_NAME=row[1], PROJECT_ID=row[2], SESSION_ID=row[3], SESSION_MODE=row[4], SESSION_DRX_BEAM=row[5], CONFIG_FILE=row[6], CAL_DIR=row[7], STATUS=row[8]) 
                for row in obs.read_sessions()]

    # Render the data into three tables
    return templates.TemplateResponse("combined.html", {"request": request, "calibrations": calibrations,
                                                        "settings": settings, "sessions": sessions})