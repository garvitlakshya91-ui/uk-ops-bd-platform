import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.models.models import PlanningApplication, Council
from app.api.auth import get_current_user, require_role
from app.models.user import User

router = APIRouter(prefix="/api/applications", tags=["Planning Applications"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class CouncilBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    region: Optional[str] = None


class ApplicationBase(BaseModel):
    reference: str
    council_id: int
    address: Optional[str] = None
    postcode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description: Optional[str] = None
    applicant_name: Optional[str] = None
    applicant_company_id: Optional[int] = None
    agent_name: Optional[str] = None
    agent_company_id: Optional[int] = None
    application_type: Optional[str] = None
    status: Optional[str] = None
    scheme_type: str = "Unknown"
    num_units: Optional[int] = None
    submission_date: Optional[datetime.date] = None
    decision_date: Optional[datetime.date] = None
    appeal_status: Optional[str] = None
    documents_url: Optional[str] = None


class ApplicationCreate(ApplicationBase):
    pass


class ApplicationUpdate(BaseModel):
    address: Optional[str] = None
    postcode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description: Optional[str] = None
    applicant_name: Optional[str] = None
    applicant_company_id: Optional[int] = None
    agent_name: Optional[str] = None
    agent_company_id: Optional[int] = None
    application_type: Optional[str] = None
    status: Optional[str] = None
    scheme_type: Optional[str] = None
    num_units: Optional[int] = None
    submission_date: Optional[datetime.date] = None
    decision_date: Optional[datetime.date] = None
    appeal_status: Optional[str] = None
    documents_url: Optional[str] = None


class ApplicationResponse(ApplicationBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    council: Optional[CouncilBrief] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime


class ApplicationListResponse(BaseModel):
    items: list[ApplicationResponse]
    total: int
    skip: int
    limit: int


class SchemeTypeCount(BaseModel):
    scheme_type: str
    count: int


class StatusCount(BaseModel):
    status: str
    count: int


class CouncilCount(BaseModel):
    council_id: int
    council_name: str
    count: int


class ApplicationStats(BaseModel):
    total: int
    by_scheme_type: list[SchemeTypeCount]
    by_status: list[StatusCount]
    by_council: list[CouncilCount]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=ApplicationListResponse)
def list_applications(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    council_id: Optional[int] = None,
    scheme_type: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    postcode: Optional[str] = None,
    date_from: Optional[datetime.date] = None,
    date_to: Optional[datetime.date] = None,
    min_units: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    query = db.query(PlanningApplication).options(joinedload(PlanningApplication.council))

    if council_id is not None:
        query = query.filter(PlanningApplication.council_id == council_id)
    if scheme_type is not None:
        query = query.filter(PlanningApplication.scheme_type == scheme_type)
    if status is not None:
        query = query.filter(PlanningApplication.status == status)
    if postcode is not None:
        query = query.filter(PlanningApplication.postcode.ilike(f"{postcode}%"))
    if date_from is not None:
        query = query.filter(PlanningApplication.submission_date >= date_from)
    if date_to is not None:
        query = query.filter(PlanningApplication.submission_date <= date_to)
    if min_units is not None:
        query = query.filter(PlanningApplication.num_units >= min_units)
    if search is not None:
        pattern = f"%{search}%"
        query = query.filter(
            or_(
                PlanningApplication.reference.ilike(pattern),
                PlanningApplication.address.ilike(pattern),
                PlanningApplication.description.ilike(pattern),
                PlanningApplication.applicant_name.ilike(pattern),
            )
        )

    total = query.count()
    items = (
        query.order_by(PlanningApplication.submission_date.desc().nullslast())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return ApplicationListResponse(items=items, total=total, skip=skip, limit=limit)


@router.get("/recent", response_model=list[ApplicationResponse])
def recent_applications(
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    items = (
        db.query(PlanningApplication)
        .options(joinedload(PlanningApplication.council))
        .order_by(PlanningApplication.created_at.desc())
        .limit(limit)
        .all()
    )
    return items


@router.get("/stats", response_model=ApplicationStats)
def application_stats(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    total = db.query(func.count(PlanningApplication.id)).scalar() or 0

    by_scheme_type = [
        SchemeTypeCount(scheme_type=row[0], count=row[1])
        for row in db.query(
            PlanningApplication.scheme_type,
            func.count(PlanningApplication.id),
        )
        .group_by(PlanningApplication.scheme_type)
        .order_by(func.count(PlanningApplication.id).desc())
        .all()
    ]

    by_status = [
        StatusCount(status=row[0] or "Unknown", count=row[1])
        for row in db.query(
            PlanningApplication.status,
            func.count(PlanningApplication.id),
        )
        .group_by(PlanningApplication.status)
        .order_by(func.count(PlanningApplication.id).desc())
        .all()
    ]

    by_council = [
        CouncilCount(council_id=row[0], council_name=row[1], count=row[2])
        for row in db.query(
            Council.id,
            Council.name,
            func.count(PlanningApplication.id),
        )
        .join(PlanningApplication, PlanningApplication.council_id == Council.id)
        .group_by(Council.id, Council.name)
        .order_by(func.count(PlanningApplication.id).desc())
        .limit(20)
        .all()
    ]

    return ApplicationStats(
        total=total,
        by_scheme_type=by_scheme_type,
        by_status=by_status,
        by_council=by_council,
    )


@router.get("/{application_id}", response_model=ApplicationResponse)
def get_application(application_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    app = (
        db.query(PlanningApplication)
        .options(joinedload(PlanningApplication.council))
        .filter(PlanningApplication.id == application_id)
        .first()
    )
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    return app


@router.post("", response_model=ApplicationResponse, status_code=status.HTTP_201_CREATED)
def create_application(data: ApplicationCreate, current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")), db: Session = Depends(get_db)):
    # Check council exists
    council = db.query(Council).filter(Council.id == data.council_id).first()
    if not council:
        raise HTTPException(status_code=400, detail="Council not found")

    # Check uniqueness
    existing = (
        db.query(PlanningApplication)
        .filter(
            PlanningApplication.reference == data.reference,
            PlanningApplication.council_id == data.council_id,
        )
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail="Application with this reference already exists for this council",
        )

    application = PlanningApplication(**data.model_dump())
    db.add(application)
    db.commit()
    db.refresh(application)

    # Eager-load council for response
    db.refresh(application, ["council"])
    return application


@router.put("/{application_id}", response_model=ApplicationResponse)
def update_application(
    application_id: int,
    data: ApplicationUpdate,
    current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")),
    db: Session = Depends(get_db),
):
    application = (
        db.query(PlanningApplication)
        .filter(PlanningApplication.id == application_id)
        .first()
    )
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(application, field, value)

    db.commit()
    db.refresh(application, ["council"])
    return application


@router.delete("/{application_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_application(application_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    application = (
        db.query(PlanningApplication)
        .filter(PlanningApplication.id == application_id)
        .first()
    )
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")

    db.delete(application)
    db.commit()
