from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi import HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from observing import obsstate as obs
import os
import fnmatch
import logging
from astropy import time
logging.basicConfig(level=logging.INFO)  # This configures the root logger
logger = logging.getLogger(__name__)

app = FastAPI()
image_dir = '/opt/devel/pipeline/images'
_DEFAULT_EVENTS_ROOT = "/opt/devel/pipeline/event_pngs"
PIPELINE_EVENTS_ROOT = os.environ.get(
    "PIPELINE_EVENT_IMAGE_ROOT", _DEFAULT_EVENTS_ROOT
)

app.mount("/static", StaticFiles(directory=image_dir), name="static")
templates = Jinja2Templates(directory="templates")


def _list_pipeline_events(root: Path):
    """Return [{ name, images: [{ name, href }] }] for direct child dirs, sorted by name."""
    if not root.is_dir():
        return []

    events = []
    for entry in sorted(root.iterdir(), key=lambda p: p.name.lower()):
        if not entry.is_dir():
            continue
        pngs = sorted(
            f.name
            for f in entry.iterdir()
            if f.is_file() and f.suffix.lower() == ".png"
        )
        q_event = quote(entry.name, safe="")
        events.append(
            {
                "name": entry.name,
                "images": [
                    {
                        "name": fn,
                        "href": f"/events/{q_event}/{quote(fn, safe='')}",
                    }
                    for fn in pngs
                ],
            }
        )
    return events


def _pipeline_event_png_full_path(events_root: str, event: str, filename: str):
    """Resolve a PNG under events_root; symlinks allowed (same rules as Starlette follow_symlink=True)."""
    if not filename.lower().endswith(".png"):
        return None
    for part in (event, filename):
        if not part or part in (".", ".."):
            return None
        if os.sep in part or (os.altsep and os.altsep in part):
            return None
    directory = os.path.abspath(events_root)
    joined = os.path.join(directory, event, filename)
    full_path = os.path.abspath(joined)
    try:
        if os.path.commonpath([full_path, directory]) != directory:
            return None
    except ValueError:
        return None
    if not os.path.isfile(full_path):
        return None
    return full_path


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
    images = [f for f in os.listdir(image_dir) if fnmatch.fnmatch(f, '*.png') or fnmatch.fnmatch(f, '*.gif') or fnmatch.fnmatch(f, '*.jpg')]

    return templates.TemplateResponse("images.html", {"request": request, "images": images})


@app.get("/pipeline-events", response_class=HTMLResponse)
async def get_pipeline_events(request: Request):
    """Browse PNGs under PIPELINE_EVENT_IMAGE_ROOT, grouped by event subdirectory."""
    root = Path(PIPELINE_EVENTS_ROOT)
    events = _list_pipeline_events(root)
    return templates.TemplateResponse(
        "pipeline_events.html",
        {
            "request": request,
            "events": events,
            "root_path": str(root.resolve()),
        },
    )


@app.get("/events/{event}/{filename}")
async def serve_pipeline_event_png(event: str, filename: str):
    full = _pipeline_event_png_full_path(PIPELINE_EVENTS_ROOT, event, filename)
    if full is None:
        raise HTTPException(status_code=404)
    return FileResponse(full, media_type="image/png")


@app.get("/files/{filename}", response_class=FileResponse)
async def download_html_file(filename: str):
    print(f"Requested file: {filename}")
    print('os.getcwd():', os.getcwd())
    return FileResponse(image_dir, media_type='text/html', filename=filename)


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

    # Calculate the time in MJD and as a date string
    current_time = time.Time.now()
    mjd = current_time.mjd
    date_string = current_time.strftime("%Y-%m-%d %H:%M:%S")

    # Render the data into three tables
    return templates.TemplateResponse("combined.html", {"request": request, "calibrations": calibrations,
                                                        "settings": settings, "sessions": sessions,
                                                        "mjd": mjd, "date_string": date_string})


_events_root = Path(PIPELINE_EVENTS_ROOT)
_events_root.mkdir(parents=True, exist_ok=True)
