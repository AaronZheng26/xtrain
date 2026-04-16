from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import get_settings
from app.db.session import initialize_database
from app.services.bootstrap import seed_demo_data
from app.services.cleanup import garbage_collect_artifact_files
from app.services.job_manager import job_manager
from app.db.session import SessionLocal


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    seed_demo_data()
    with SessionLocal() as db:
        garbage_collect_artifact_files(db)
    job_manager.start()
    yield
    job_manager.shutdown()


settings = get_settings()
app = FastAPI(title=settings.app_name, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.api_prefix)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "xtrain backend is running"}
