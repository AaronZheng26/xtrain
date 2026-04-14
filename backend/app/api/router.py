from fastapi import APIRouter

from app.api.routes import analysis, dashboard, datasets, feature_templates, features, health, import_sessions, jobs, preprocess, projects, training


api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
api_router.include_router(import_sessions.router, prefix="/import-sessions", tags=["import-sessions"])
api_router.include_router(preprocess.router, prefix="/pipelines", tags=["pipelines"])
api_router.include_router(features.router, prefix="/pipelines", tags=["pipelines"])
api_router.include_router(feature_templates.router, prefix="/features", tags=["features"])
api_router.include_router(training.router, prefix="/training", tags=["training"])
api_router.include_router(analysis.router, prefix="/analysis", tags=["analysis"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
