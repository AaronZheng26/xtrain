from sqlalchemy import func, select

from app.db.session import SessionLocal
from app.models.project import Project


def seed_demo_data() -> None:
    with SessionLocal() as db:
        count = db.scalar(select(func.count()).select_from(Project)) or 0
        if count > 0:
            return

        demo = Project(
            name="SOC Demo",
            description="面向安全分析师的日志分析工作区示例。",
        )
        db.add(demo)
        db.commit()
