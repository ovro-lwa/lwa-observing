from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['GET'],
        allow_headers=["*"],
    )

app.mount("/data", StaticFiles(directory="/opt/devel/pipeline/images", html=True), name="static")
