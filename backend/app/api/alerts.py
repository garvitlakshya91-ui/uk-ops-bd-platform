import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.models import Alert
from app.api.auth import get_current_user, require_role
from app.models.user import User
from app.api.permissions import get_allowed_alert_types

router = APIRouter(prefix="/api/alerts", tags=["Alerts"])

VALID_ALERT_TYPES = [
    "new_application",
    "status_change",
    "contract_expiring",
    "scraper_failure",
    "new_opportunity",
]


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class AlertResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    type: str
    title: str
    message: Optional[str] = None
    entity_type: Optional[str] = None
    entity_id: Optional[int] = None
    is_read: bool
    created_at: datetime.datetime


class AlertListResponse(BaseModel):
    items: list[AlertResponse]
    total: int
    skip: int
    limit: int
    unread_count: int


class AlertCreate(BaseModel):
    type: str
    title: str
    message: Optional[str] = None
    entity_type: Optional[str] = None
    entity_id: Optional[int] = None


class MarkReadRequest(BaseModel):
    alert_ids: list[int]


class AlertPreferences(BaseModel):
    """User alert preferences (stored per-user in a real implementation;
    returned as a static config here for the API contract)."""
    new_application: bool = True
    status_change: bool = True
    contract_expiring: bool = True
    scraper_failure: bool = True
    new_opportunity: bool = True
    email_enabled: bool = True
    slack_enabled: bool = False


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=AlertListResponse)
def list_alerts(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    alert_type: Optional[str] = None,
    is_read: Optional[bool] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    query = db.query(Alert)

    # Filter alert types by role
    allowed_types = get_allowed_alert_types(current_user)
    if allowed_types is not None:
        query = query.filter(Alert.type.in_(allowed_types))

    if alert_type is not None:
        query = query.filter(Alert.type == alert_type)
    if is_read is not None:
        query = query.filter(Alert.is_read == is_read)

    total = query.count()
    unread_count = (
        db.query(func.count(Alert.id)).filter(Alert.is_read == False).scalar() or 0  # noqa: E712
    )
    items = query.order_by(Alert.created_at.desc()).offset(skip).limit(limit).all()

    return AlertListResponse(
        items=items, total=total, skip=skip, limit=limit, unread_count=unread_count
    )


@router.get("/{alert_id}", response_model=AlertResponse)
def get_alert(alert_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@router.post("", response_model=AlertResponse, status_code=status.HTTP_201_CREATED)
def create_alert(data: AlertCreate, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    if data.type not in VALID_ALERT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid alert type. Must be one of: {VALID_ALERT_TYPES}",
        )

    alert = Alert(**data.model_dump())
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert


@router.put("/{alert_id}/read", response_model=AlertResponse)
def mark_alert_read(alert_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.is_read = True
    db.commit()
    db.refresh(alert)
    return alert


@router.post("/mark-read", response_model=dict)
def mark_multiple_read(data: MarkReadRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    updated = (
        db.query(Alert)
        .filter(Alert.id.in_(data.alert_ids))
        .update({"is_read": True}, synchronize_session="fetch")
    )
    db.commit()
    return {"marked_read": updated}


@router.post("/mark-all-read", response_model=dict)
def mark_all_read(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    updated = (
        db.query(Alert)
        .filter(Alert.is_read == False)  # noqa: E712
        .update({"is_read": True}, synchronize_session="fetch")
    )
    db.commit()
    return {"marked_read": updated}


@router.delete("/{alert_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_alert(alert_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    db.delete(alert)
    db.commit()


@router.get("/preferences/current", response_model=AlertPreferences)
def get_alert_preferences(current_user: User = Depends(get_current_user)):
    """Return current alert preferences.

    In a full implementation this would be per-user and persisted to the
    database. Here we return sensible defaults so the frontend can render
    the preferences UI.
    """
    return AlertPreferences()


@router.put("/preferences/current", response_model=AlertPreferences)
def update_alert_preferences(prefs: AlertPreferences, current_user: User = Depends(get_current_user)):
    """Update alert preferences.

    In a full implementation this would persist to the database.
    """
    return prefs
