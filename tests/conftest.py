from collections.abc import Iterator
from uuid import UUID

import pytest
from sqlalchemy import delete
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from app.db import SessionLocal, get_db
from app.main import app
from app.models import Alert, AlertHistory, AlertRule, Reading


@pytest.fixture
def db_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


@pytest.fixture
def client() -> Iterator[TestClient]:
    def override_get_db() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
def cleanup_rows(db_session: Session):
    readings: list[UUID] = []
    rules: list[UUID] = []
    alerts: list[UUID] = []

    def track(*, reading: UUID | None = None, rule: UUID | None = None, alert: UUID | None = None) -> None:
        if reading:
            readings.append(reading)
        if rule:
            rules.append(rule)
        if alert:
            alerts.append(alert)

    yield track

    if alerts:
        db_session.execute(delete(AlertHistory).where(AlertHistory.alert_id.in_(alerts)))
        db_session.execute(delete(Alert).where(Alert.id.in_(alerts)))
    if rules:
        db_session.execute(delete(Alert).where(Alert.rule_id.in_(rules)))
        db_session.execute(delete(AlertRule).where(AlertRule.id.in_(rules)))
    if readings:
        db_session.execute(delete(Reading).where(Reading.event_id.in_(readings)))
    db_session.commit()
