import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, Callable

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.models.feature_pipeline import FeaturePipeline
from app.models.job import Job
from app.models.model_version import ModelVersion
from app.models.preprocess_advisor_run import PreprocessAdvisorRun
from app.models.preprocess_pipeline import PreprocessPipeline


INTERRUPTED_STATUSES = {"queued", "running"}
INTERRUPTED_MESSAGE = "Interrupted by backend restart; please resubmit the task."


class JobManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None

    def start(self) -> None:
        settings = get_settings()
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)

    def shutdown(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=False)
            self._executor = None

    def recover_interrupted_jobs(self) -> int:
        with self._lock:
            with SessionLocal() as db:
                now = datetime.now(UTC)
                interrupted_jobs = (
                    db.query(Job)
                    .filter(Job.status.in_(INTERRUPTED_STATUSES))
                    .all()
                )
                for job in interrupted_jobs:
                    job.status = "failed"
                    job.progress = 100
                    job.message = INTERRUPTED_MESSAGE
                    job.updated_at = now
                    db.add(job)

                for model in db.query(ModelVersion).filter(ModelVersion.status.in_(INTERRUPTED_STATUSES)).all():
                    model.status = "failed"
                    model.updated_at = now
                    db.add(model)
                for pipeline in db.query(PreprocessPipeline).filter(PreprocessPipeline.status.in_(INTERRUPTED_STATUSES)).all():
                    pipeline.status = "failed"
                    pipeline.updated_at = now
                    db.add(pipeline)
                for pipeline in db.query(FeaturePipeline).filter(FeaturePipeline.status.in_(INTERRUPTED_STATUSES)).all():
                    pipeline.status = "failed"
                    pipeline.updated_at = now
                    db.add(pipeline)
                for advisor_run in db.query(PreprocessAdvisorRun).filter(PreprocessAdvisorRun.status.in_(INTERRUPTED_STATUSES)).all():
                    advisor_run.status = "failed"
                    advisor_run.updated_at = now
                    db.add(advisor_run)

                db.commit()
                return len(interrupted_jobs)

    def submit_demo_job(self, job_id: int, duration_seconds: int) -> None:
        if self._executor is None:
            self.start()
        assert self._executor is not None
        self._executor.submit(self._run_demo_job, job_id, max(duration_seconds, 1))

    def submit_task(self, func: Callable[..., Any], *args: Any) -> None:
        if self._executor is None:
            self.start()
        assert self._executor is not None
        self._executor.submit(func, *args)

    def _run_demo_job(self, job_id: int, duration_seconds: int) -> None:
        self._update_job(job_id, status="running", progress=5, message="Preparing training resources")
        steps = max(duration_seconds, 4)

        for index in range(1, steps + 1):
            time.sleep(duration_seconds / steps)
            progress = min(95, int(index / steps * 100))
            self._update_job(
                job_id,
                status="running",
                progress=progress,
                message=f"Executing training stage {index}/{steps}",
            )

        self._update_job(job_id, status="completed", progress=100, message="Demo training finished")

    def _update_job(self, job_id: int, status: str, progress: int, message: str) -> None:
        with self._lock:
            with SessionLocal() as db:
                job = db.get(Job, job_id)
                if not job:
                    return
                job.status = status
                job.progress = progress
                job.message = message
                job.updated_at = datetime.now(UTC)
                db.add(job)
                db.commit()


job_manager = JobManager()
