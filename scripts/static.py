import os
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates

_DEFAULT_EVENTS_ROOT = "/opt/devel/pipeline/event_pngs"
PIPELINE_EVENTS_ROOT = os.environ.get(
    "PIPELINE_EVENT_IMAGE_ROOT", _DEFAULT_EVENTS_ROOT
)

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


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


@app.get("/events/{event}/{filename}")
async def serve_pipeline_event_png(event: str, filename: str):
    full = _pipeline_event_png_full_path(PIPELINE_EVENTS_ROOT, event, filename)
    if full is None:
        raise HTTPException(status_code=404)
    return FileResponse(full, media_type="image/png")


@app.get("/")
async def pipeline_events_landing(request: Request):
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


app.mount(
    "/data",
    StaticFiles(directory="/opt/devel/pipeline/images", html=True),
    name="legacy_images",
)
events_root = Path(PIPELINE_EVENTS_ROOT)
events_root.mkdir(parents=True, exist_ok=True)
