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
image_dir = '/opt/devel/pipeline/images'
app.mount("/static", StaticFiles(directory=image_dir), name="static")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
async def startup_event():
    """Create database on startup."""
    obs.create_db()


@app.get("/sessions", response_class=HTMLResponse)
async def get_all_sessions(request: Request):
    rows = obs.read_sessions()
    sessions = [obs.Session(time_loaded=row[0], PI_ID=row[1], PI_NAME=row[2], PROJECT_ID=row[3],
                            SESSION_ID=row[4], SESSION_MODE=row[5], SESSION_DRX_BEAM=row[6],
                            CONFIG_FILE=row[7], CAL_DIR=row[8], STATUS=row[9]) for row in rows]
    return templates.TemplateResponse("sessions.html", {"request": request, "sessions": sessions})


@app.get("/settings", response_class=HTMLResponse)
async def get_settings(request: Request):
    rows = obs.read_settings()
    settings = [obs.Settings(time_loaded=row[0], user=row[1], filename=row[2]) for row in rows]
    return templates.TemplateResponse("settings.html", {"request": request, "settings": settings})


@app.get("/calibrations", response_class=HTMLResponse)
async def get_calibrations(request: Request):
    rows = obs.read_calibrations()
    calibrations = [obs.Calibrations(time_loaded=row[0], filename=row[1], beam=row[2]) for row in rows]
    return templates.TemplateResponse("calibrations.html", {"request": request, "calibrations": calibrations})


@app.get("/images", response_class=HTMLResponse)
async def get_images(request: Request):
    images = [f for f in os.listdir(image_dir) if fnmatch.fnmatch(f, '*.png')]
    grouped_images = [all_images[i:i+12] for i in range(0, len(all_images), 12)]

    return templates.TemplateResponse("images.html", {"request": request, "grouped_images": grouped_images})


@app.get("/", response_class=HTMLResponse)
async def get_combined(request: Request):
    # Fetch data from the calibrations table
    calibrations = [obs.Calibrations(time_loaded=row[0], filename=row[1], beam=row[2]) 
                    for row in obs.read_calibrations()]

    # Fetch data from the settings table
    settings = [obs.Settings(time_loaded=row[0], user=row[1], filename=row[2]) 
                for row in obs.read_settings()]

    # Fetch data from the sessions table
    sessions = [obs.Session(time_loaded=row[0], PI_ID=row[1], PI_NAME=row[2], PROJECT_ID=row[3],
                            SESSION_ID=row[4], SESSION_MODE=row[5], SESSION_DRX_BEAM=row[6],
                            CONFIG_FILE=row[7], CAL_DIR=row[8], STATUS=row[9]) for row in obs.read_sessions()]

    # Render the data into three tables
    return templates.TemplateResponse("combined.html", {"request": request, "calibrations": calibrations,
                                                        "settings": settings, "sessions": sessions})