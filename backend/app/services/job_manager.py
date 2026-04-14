import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.models.job import Job


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

    def submit_demo_job(self, job_id: int, duration_seconds: int) -> None:
        if self._executor is None:
            self.start()
        assert self._executor is not None
        self._executor.submit(self._run_demo_job, job_id, max(duration_seconds, 1))

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
