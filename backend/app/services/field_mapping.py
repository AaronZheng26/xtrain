from sqlalchemy.orm import Session

from app.models.dataset_version import DatasetVersion
from app.models.field_mapping import FieldMapping
from app.services.dataset_import import get_dataset

STANDARD_FIELDS = [
    "event_time",
    "source_ip",
    "dest_ip",
    "status_code",
    "label",
    "raw_message",
]


def get_or_create_field_mapping(db: Session, dataset_id: int) -> FieldMapping:
    dataset = get_dataset(db, dataset_id)
    field_mapping = db.query(FieldMapping).filter(FieldMapping.dataset_version_id == dataset_id).first()
    if field_mapping:
        return field_mapping

    field_mapping = FieldMapping(
        dataset_version_id=dataset_id,
        mappings=_suggest_mapping(dataset),
        confirmed=False,
    )
    db.add(field_mapping)
    db.commit()
    db.refresh(field_mapping)
    return field_mapping


def update_field_mapping(db: Session, dataset_id: int, mappings: dict[str, str | None]) -> FieldMapping:
    dataset = get_dataset(db, dataset_id)
    valid_columns = {field["name"] for field in dataset.schema_snapshot}
    sanitized: dict[str, str | None] = {}
    for key in STANDARD_FIELDS:
        value = mappings.get(key)
        if value is not None and value not in valid_columns:
            raise ValueError(f"Column '{value}' does not exist in dataset")
        sanitized[key] = value

    field_mapping = get_or_create_field_mapping(db, dataset_id)
    field_mapping.mappings = sanitized
    field_mapping.confirmed = any(value for value in sanitized.values())
    db.add(field_mapping)
    db.commit()
    db.refresh(field_mapping)
    return field_mapping


def _suggest_mapping(dataset: DatasetVersion) -> dict[str, str | None]:
    columns = [field["name"] for field in dataset.schema_snapshot]
    lower_lookup = {column.lower(): column for column in columns}
    detected = dataset.detected_fields or {}

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            if candidate in lower_lookup:
                return lower_lookup[candidate]
        return None

    def pick_contains(*needles: str) -> str | None:
        for column in columns:
            lowered = column.lower()
            if all(needle in lowered for needle in needles):
                return column
        return None

    mapping = {
        "event_time": (detected.get("timestamp_candidates") or [None])[0],
        "source_ip": pick("source_ip", "src_ip", "remote_addr") or pick_contains("source", "ip") or pick_contains("src", "ip"),
        "dest_ip": pick("dest_ip", "dst_ip") or pick_contains("dest", "ip") or pick_contains("dst", "ip"),
        "status_code": pick("status_code", "status"),
        "label": (detected.get("label_candidates") or [None])[0],
        "raw_message": pick("raw_message", "raw_line", "message"),
    }

    return mapping
