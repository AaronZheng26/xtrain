import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db.base import Base
from app.models.llm_provider_config import LlmProviderConfig
from app.models.project import Project
from app.schemas.analysis import LlmProviderConfigUpdate
from app.services.llm_analysis import VOLATILE_API_KEYS, serialize_llm_config, update_llm_config


class LlmConfigTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(bind=engine)
        self.SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        VOLATILE_API_KEYS.clear()

    def tearDown(self):
        VOLATILE_API_KEYS.clear()

    def test_update_masks_api_key_in_database(self):
        with self.SessionLocal() as db:
            project = Project(name="demo", description="demo", status="active")
            db.add(project)
            db.commit()
            db.refresh(project)

            config = update_llm_config(
                db,
                project.id,
                LlmProviderConfigUpdate(
                    provider="minimax",
                    enabled=True,
                    base_url="https://api.minimaxi.com/v1",
                    model_name="MiniMax-M2.5",
                    api_key="sk-secret-12345678",
                    clear_api_key=False,
                ),
            )

            persisted = db.get(LlmProviderConfig, config.id)
            self.assertEqual(persisted.api_key, "sk-s...5678")
            self.assertNotEqual(persisted.api_key, "sk-secret-12345678")

            serialized = serialize_llm_config(persisted)
            self.assertTrue(serialized["has_api_key"])
            self.assertEqual(serialized["api_key_hint"], "sk-s...5678")

    def test_env_var_is_preferred_for_runtime_key_resolution(self):
        with self.SessionLocal() as db:
            project = Project(name="demo", description="demo", status="active")
            db.add(project)
            db.commit()
            db.refresh(project)

            config = update_llm_config(
                db,
                project.id,
                LlmProviderConfigUpdate(
                    provider="openai_compatible",
                    enabled=True,
                    base_url="https://api.example.com/v1",
                    model_name="gpt-4o-mini",
                    api_key="sk-local-cache",
                    clear_api_key=False,
                ),
            )
            VOLATILE_API_KEYS.clear()

            with patch.dict(os.environ, {"XTRAIN_PROJECT_1_OPENAI_COMPATIBLE_API_KEY": "sk-env-value"}, clear=False):
                serialized = serialize_llm_config(config)

            self.assertTrue(serialized["has_api_key"])
            self.assertEqual(serialized["api_key_hint"], "sk-l...ache")


if __name__ == "__main__":
    unittest.main()
