from uuid import uuid4

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import AlertRule
from app.schemas import AlertRuleCreate, AlertRuleResponse

router = APIRouter(prefix="/rules", tags=["rules"])


@router.post("", response_model=AlertRuleResponse, status_code=status.HTTP_201_CREATED)
def create_rule(payload: AlertRuleCreate, db: Session = Depends(get_db)) -> AlertRuleResponse:
    rule = AlertRule(
        id=uuid4(),
        name=payload.name,
        sensor_id=payload.sensorId,
        metric_type=payload.type,
        operator=payload.operator,
        threshold=payload.threshold,
        window_seconds=payload.windowSeconds,
        aggregation=payload.aggregation,
        enabled=payload.enabled,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return AlertRuleResponse.from_model(rule)


@router.get("", response_model=list[AlertRuleResponse])
def list_rules(
    enabled: bool | None = None,
    sensor_id: str | None = Query(default=None, alias="sensorId", min_length=1, max_length=128),
    metric_type: str | None = Query(default=None, alias="type", min_length=1, max_length=64),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[AlertRuleResponse]:
    query = select(AlertRule)
    if enabled is not None:
        query = query.where(AlertRule.enabled.is_(enabled))
    if sensor_id is not None:
        query = query.where((AlertRule.sensor_id == sensor_id) | (AlertRule.sensor_id.is_(None)))
    if metric_type is not None:
        query = query.where(AlertRule.metric_type == metric_type)
    rules = db.scalars(
        query.order_by(AlertRule.created_at.desc(), AlertRule.id).limit(limit).offset(offset)
    ).all()
    return [AlertRuleResponse.from_model(rule) for rule in rules]
