from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.job import Job
from app.schemas.job import DemoJobRequest, JobRead
from app.services.job_manager import job_manager


router = APIRouter()


@router.get("", response_model=list[JobRead])
def list_jobs(db: Session = Depends(get_db)) -> list[Job]:
    return list(db.scalars(select(Job).order_by(Job.created_at.desc())))


@router.get("/{job_id}", response_model=JobRead)
def get_job(job_id: int, db: Session = Depends(get_db)) -> Job:
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/demo", response_model=JobRead, status_code=status.HTTP_202_ACCEPTED)
def create_demo_job(payload: DemoJobRequest, db: Session = Depends(get_db)) -> Job:
    job = Job(
        name=payload.name,
        job_type="training",
        status="queued",
        progress=0,
        message="Waiting to start",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    job_manager.submit_demo_job(job.id, payload.duration_seconds)
    return job
