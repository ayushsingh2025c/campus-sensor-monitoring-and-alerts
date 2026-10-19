from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Alert, AlertHistory
from app.schemas import AlertHistoryResponse, AlertResponse, AlertStatus

router = APIRouter(prefix="/alerts", tags=["alerts"])


def _alert_response(alert: Alert, history: list[AlertHistory] | None = None) -> AlertResponse:
    return AlertResponse(
        id=alert.id,
        ruleId=alert.rule_id,
        sensorId=alert.sensor_id,
        status=alert.status,
        firstTriggeredAt=alert.first_triggered_at,
        lastTriggeredAt=alert.last_triggered_at,
        acknowledgedAt=alert.acknowledged_at,
        resolvedAt=alert.resolved_at,
        history=[
            AlertHistoryResponse(
                fromStatus=entry.from_status,
                toStatus=entry.to_status,
                changedAt=entry.changed_at,
            )
            for entry in history or []
        ],
    )


def _get_alert(alert_id: UUID, db: Session, *, lock: bool = False) -> Alert:
    query = select(Alert).where(Alert.id == alert_id)
    if lock:
        query = query.with_for_update()
    alert = db.scalar(query)
    if alert is None:
        raise HTTPException(status_code=404, detail={"code": "alert_not_found", "message": "Alert not found"})
    return alert


@router.get("", response_model=list[AlertResponse])
def list_alerts(
    alert_status: AlertStatus | None = Query(default=None, alias="status"),
    sensor_id: str | None = Query(default=None, alias="sensorId", min_length=1, max_length=128),
    rule_id: UUID | None = Query(default=None, alias="ruleId"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[AlertResponse]:
    query = select(Alert)
    if alert_status is not None:
        query = query.where(Alert.status == alert_status)
    if sensor_id is not None:
        query = query.where(Alert.sensor_id == sensor_id)
    if rule_id is not None:
        query = query.where(Alert.rule_id == rule_id)
    alerts = db.scalars(
        query.order_by(Alert.last_triggered_at.desc(), Alert.id).limit(limit).offset(offset)
    ).all()
    if not alerts:
        return []
    histories = db.scalars(
        select(AlertHistory)
        .where(AlertHistory.alert_id.in_([alert.id for alert in alerts]))
        .order_by(AlertHistory.changed_at, AlertHistory.id)
    ).all()
    histories_by_alert: dict[UUID, list[AlertHistory]] = {}
    for entry in histories:
        histories_by_alert.setdefault(entry.alert_id, []).append(entry)
    return [_alert_response(alert, histories_by_alert.get(alert.id)) for alert in alerts]


@router.get("/{alert_id}", response_model=AlertResponse)
def get_alert(alert_id: UUID, db: Session = Depends(get_db)) -> AlertResponse:
    alert = _get_alert(alert_id, db)
    history = db.scalars(
        select(AlertHistory)
        .where(AlertHistory.alert_id == alert.id)
        .order_by(AlertHistory.changed_at, AlertHistory.id)
    ).all()
    return _alert_response(alert, history)


def _transition(alert_id: UUID, to_status: str, db: Session) -> AlertResponse:
    alert = _get_alert(alert_id, db, lock=True)
    expected = "OPEN" if to_status == "ACKNOWLEDGED" else "ACKNOWLEDGED"
    if alert.status != expected:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "invalid_alert_transition",
                "message": f"Cannot transition alert from {alert.status} to {to_status}",
            },
        )
    now = datetime.now(UTC)
    previous_status = alert.status
    alert.status = to_status
    if to_status == "ACKNOWLEDGED":
        alert.acknowledged_at = now
    else:
        alert.resolved_at = now
    db.add(
        AlertHistory(
            alert_id=alert.id,
            from_status=previous_status,
            to_status=to_status,
            changed_at=now,
        )
    )
    db.commit()
    db.refresh(alert)
    history = db.scalars(
        select(AlertHistory)
        .where(AlertHistory.alert_id == alert.id)
        .order_by(AlertHistory.changed_at, AlertHistory.id)
    ).all()
    return _alert_response(alert, history)


@router.post("/{alert_id}/acknowledge", response_model=AlertResponse)
def acknowledge_alert(alert_id: UUID, db: Session = Depends(get_db)) -> AlertResponse:
    return _transition(alert_id, "ACKNOWLEDGED", db)


@router.post("/{alert_id}/resolve", response_model=AlertResponse)
def resolve_alert(alert_id: UUID, db: Session = Depends(get_db)) -> AlertResponse:
    return _transition(alert_id, "RESOLVED", db)
