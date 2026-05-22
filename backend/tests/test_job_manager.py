import sys
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.models  # noqa: F401
from app.db.base import Base
from app.models.feature_pipeline import FeaturePipeline
from app.models.job import Job
from app.models.model_version import ModelVersion
from app.models.preprocess_advisor_run import PreprocessAdvisorRun
from app.models.preprocess_pipeline import PreprocessPipeline
from app.services import job_manager as job_manager_module
from app.services.job_manager import JobManager


class JobRecoveryTests(unittest.TestCase):
    def test_recover_interrupted_jobs_marks_jobs_and_resources_failed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "jobs.db"
            engine = create_engine(
                f"sqlite:///{db_path}",
                connect_args={"check_same_thread": False},
                poolclass=NullPool,
            )
            Base.metadata.create_all(bind=engine)
            temp_session_local = sessionmaker(bind=engine, autoflush=False, autocommit=False)

            with temp_session_local() as db:
                db.add(Job(name="preprocess", job_type="preprocess", status="running", progress=40, message="running"))
                db.add(Job(name="feature", job_type="feature", status="queued", progress=0, message="queued"))
                db.add(Job(name="done", job_type="training", status="completed", progress=100, message="done"))
                db.add(
                    PreprocessPipeline(
                        project_id=1,
                        dataset_version_id=1,
                        name="preprocess",
                        status="running",
                        steps=[],
                    )
                )
                db.add(
                    FeaturePipeline(
                        project_id=1,
                        dataset_version_id=1,
                        name="feature",
                        status="queued",
                        steps=[],
                    )
                )
                db.add(
                    ModelVersion(
                        project_id=1,
                        dataset_version_id=1,
                        name="model",
                        mode="unsupervised",
                        algorithm="isolation_forest",
                        status="running",
                    )
                )
                db.add(
                    PreprocessAdvisorRun(
                        project_id=1,
                        dataset_version_id=1,
                        status="queued",
                    )
                )
                db.commit()

            original_session_local = job_manager_module.SessionLocal
            try:
                job_manager_module.SessionLocal = temp_session_local
                recovered_count = JobManager().recover_interrupted_jobs()
            finally:
                job_manager_module.SessionLocal = original_session_local
                engine.dispose()

            with temp_session_local() as db:
                jobs = {job.name: job for job in db.query(Job).all()}
                preprocess_pipeline = db.query(PreprocessPipeline).one()
                feature_pipeline = db.query(FeaturePipeline).one()
                model_version = db.query(ModelVersion).one()
                advisor_run = db.query(PreprocessAdvisorRun).one()

                self.assertEqual(recovered_count, 2)
                self.assertEqual(jobs["preprocess"].status, "failed")
                self.assertEqual(jobs["feature"].status, "failed")
                self.assertEqual(jobs["done"].status, "completed")
                self.assertEqual(preprocess_pipeline.status, "failed")
                self.assertEqual(feature_pipeline.status, "failed")
                self.assertEqual(model_version.status, "failed")
                self.assertEqual(advisor_run.status, "failed")


if __name__ == "__main__":
    unittest.main()
