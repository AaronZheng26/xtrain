from __future__ import annotations

import shutil
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.data_source import DataSource
from app.models.dataset_version import DatasetVersion
from app.models.feature_pipeline import FeaturePipeline
from app.models.feature_template import FeatureTemplate
from app.models.field_mapping import FieldMapping
from app.models.import_session import ImportSession
from app.models.job import Job
from app.models.llm_provider_config import LlmProviderConfig
from app.models.model_version import ModelVersion
from app.models.preprocess_pipeline import PreprocessPipeline
from app.models.project import Project


def delete_project_with_assets(db: Session, project_id: int) -> None:
    project = db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    datasets = list(
        db.scalars(select(DatasetVersion).where(DatasetVersion.project_id == project_id))
    )
    for dataset in datasets:
        _delete_dataset_related_records(db, dataset)

    llm_config = db.scalar(select(LlmProviderConfig).where(LlmProviderConfig.project_id == project_id))
    if llm_config:
        db.delete(llm_config)

    import_sessions = list(db.scalars(select(ImportSession).where(ImportSession.project_id == project_id)))
    for import_session in import_sessions:
        db.delete(import_session)

    feature_templates = list(db.scalars(select(FeatureTemplate).where(FeatureTemplate.project_id == project_id)))
    for feature_template in feature_templates:
        db.delete(feature_template)

    db.delete(project)
    db.commit()

    settings = get_settings()
    storage_root = settings.storage_root_path
    for relative_dir in [
        storage_root / "raw" / f"project_{project_id}",
        storage_root / "processed" / f"project_{project_id}",
        storage_root / "preprocessed" / f"project_{project_id}",
        storage_root / "features" / f"project_{project_id}",
        storage_root / "models" / f"project_{project_id}",
    ]:
        _remove_path(relative_dir, storage_root)


def delete_dataset_with_assets(db: Session, dataset_id: int) -> None:
    dataset = db.get(DatasetVersion, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    _delete_dataset_related_records(db, dataset)
    db.commit()


def _delete_dataset_related_records(db: Session, dataset: DatasetVersion) -> None:
    model_versions = list(
        db.scalars(select(ModelVersion).where(ModelVersion.dataset_version_id == dataset.id))
    )
    job_ids = [model.job_id for model in model_versions if model.job_id is not None]

    for model in model_versions:
        _remove_record_file(model.artifact_path)
        _remove_record_file(model.prediction_path)
        db.delete(model)

    feature_pipelines = list(
        db.scalars(select(FeaturePipeline).where(FeaturePipeline.dataset_version_id == dataset.id))
    )
    for pipeline in feature_pipelines:
        _remove_record_file(pipeline.output_path)
        db.delete(pipeline)

    preprocess_pipelines = list(
        db.scalars(select(PreprocessPipeline).where(PreprocessPipeline.dataset_version_id == dataset.id))
    )
    for pipeline in preprocess_pipelines:
        _remove_record_file(pipeline.output_path)
        db.delete(pipeline)

    field_mapping = db.scalar(
        select(FieldMapping).where(FieldMapping.dataset_version_id == dataset.id)
    )
    if field_mapping:
        db.delete(field_mapping)

    data_source = db.get(DataSource, dataset.source_id)
    _remove_record_file(dataset.parquet_path)
    db.delete(dataset)

    if data_source:
        remaining_dataset = db.scalar(
            select(DatasetVersion).where(
                DatasetVersion.source_id == data_source.id,
                DatasetVersion.id != dataset.id,
            )
        )
        if not remaining_dataset:
            _remove_record_file(data_source.storage_path)
            db.delete(data_source)

    if job_ids:
        jobs = list(db.scalars(select(Job).where(Job.id.in_(job_ids))))
        for job in jobs:
            db.delete(job)


def _remove_record_file(path_value: str | None) -> None:
    if not path_value:
        return
    settings = get_settings()
    storage_root = settings.storage_root_path
    _remove_path(settings.resolve_storage_path(path_value), storage_root)


def _remove_path(path: Path, storage_root: Path) -> None:
    resolved = path.resolve()
    try:
        resolved.relative_to(storage_root)
    except ValueError:
        return

    if not resolved.exists():
        return

    if resolved.is_dir():
        shutil.rmtree(resolved, ignore_errors=True)
    else:
        resolved.unlink(missing_ok=True)
