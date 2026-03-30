import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, and_
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.models.models import ExistingScheme, Company, Council, SchemeContract, SchemeChangeLog
from app.api.auth import get_current_user, require_role
from app.models.user import User

router = APIRouter(prefix="/api/schemes", tags=["Existing Schemes"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class CompanyBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    company_type: Optional[str] = None


class CouncilBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    region: Optional[str] = None


class SchemeBase(BaseModel):
    name: str
    address: Optional[str] = None
    postcode: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    council_id: Optional[int] = None
    operator_company_id: Optional[int] = None
    owner_company_id: Optional[int] = None
    scheme_type: Optional[str] = None
    num_units: Optional[int] = None
    contract_start_date: Optional[datetime.date] = None
    contract_end_date: Optional[datetime.date] = None
    performance_rating: Optional[float] = None
    satisfaction_score: Optional[float] = None
    regulatory_rating: Optional[str] = None
    financial_health_score: Optional[float] = None
    epc_ratings: Optional[dict] = None
    asset_manager_company_id: Optional[int] = None
    landlord_company_id: Optional[int] = None
    status: Optional[str] = None
    source: Optional[str] = None
    source_reference: Optional[str] = None
    last_verified_at: Optional[datetime.datetime] = None
    data_confidence_score: Optional[float] = None


class SchemeCreate(SchemeBase):
    pass


class SchemeUpdate(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    postcode: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    council_id: Optional[int] = None
    operator_company_id: Optional[int] = None
    owner_company_id: Optional[int] = None
    scheme_type: Optional[str] = None
    num_units: Optional[int] = None
    contract_start_date: Optional[datetime.date] = None
    contract_end_date: Optional[datetime.date] = None
    performance_rating: Optional[float] = None
    satisfaction_score: Optional[float] = None
    regulatory_rating: Optional[str] = None
    financial_health_score: Optional[float] = None
    epc_ratings: Optional[dict] = None
    asset_manager_company_id: Optional[int] = None
    landlord_company_id: Optional[int] = None
    status: Optional[str] = None
    source: Optional[str] = None
    source_reference: Optional[str] = None
    last_verified_at: Optional[datetime.datetime] = None
    data_confidence_score: Optional[float] = None


class SchemeResponse(SchemeBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    operator_company: Optional[CompanyBrief] = None
    owner_company: Optional[CompanyBrief] = None
    asset_manager_company: Optional[CompanyBrief] = None
    landlord_company: Optional[CompanyBrief] = None
    council: Optional[CouncilBrief] = None
    status: Optional[str] = None
    source: Optional[str] = None
    last_verified_at: Optional[datetime.datetime] = None
    data_confidence_score: Optional[float] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime


class SchemeListResponse(BaseModel):
    items: list[SchemeResponse]
    total: int
    skip: int
    limit: int


class SchemeContractResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    contract_reference: Optional[str] = None
    contract_type: Optional[str] = None
    operator_company: Optional[CompanyBrief] = None
    client_company: Optional[CompanyBrief] = None
    contract_start_date: Optional[datetime.date] = None
    contract_end_date: Optional[datetime.date] = None
    contract_value: Optional[float] = None
    currency: str = "GBP"
    source: Optional[str] = None
    source_reference: Optional[str] = None
    is_current: bool = True
    created_at: datetime.datetime


class SchemeChangeLogResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    field_name: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    source: Optional[str] = None
    changed_by: Optional[str] = None
    changed_at: datetime.datetime


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=SchemeListResponse)
def list_schemes(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    scheme_type: Optional[str] = None,
    council_id: Optional[int] = None,
    operator_company_id: Optional[int] = None,
    postcode: Optional[str] = None,
    status: Optional[str] = None,
    landlord_company_id: Optional[int] = None,
    asset_manager_company_id: Optional[int] = None,
    contract_expiry_before: Optional[datetime.date] = None,
    contract_expiry_after: Optional[datetime.date] = None,
    min_performance_rating: Optional[float] = None,
    sort_by: str = Query(
        "name",
        pattern="^(name|num_units|performance_rating|satisfaction_score|financial_health_score|contract_end_date)$",
    ),
    sort_order: str = Query("asc", pattern="^(asc|desc)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    query = db.query(ExistingScheme).options(
        joinedload(ExistingScheme.operator_company),
        joinedload(ExistingScheme.owner_company),
        joinedload(ExistingScheme.asset_manager_company),
        joinedload(ExistingScheme.landlord_company),
        joinedload(ExistingScheme.council),
    )

    if scheme_type is not None:
        query = query.filter(ExistingScheme.scheme_type == scheme_type)
    if council_id is not None:
        query = query.filter(ExistingScheme.council_id == council_id)
    if operator_company_id is not None:
        query = query.filter(ExistingScheme.operator_company_id == operator_company_id)
    if status is not None:
        query = query.filter(ExistingScheme.status == status)
    if landlord_company_id is not None:
        query = query.filter(ExistingScheme.landlord_company_id == landlord_company_id)
    if asset_manager_company_id is not None:
        query = query.filter(ExistingScheme.asset_manager_company_id == asset_manager_company_id)
    if postcode is not None:
        query = query.filter(ExistingScheme.postcode.ilike(f"{postcode}%"))
    if contract_expiry_before is not None:
        query = query.filter(ExistingScheme.contract_end_date <= contract_expiry_before)
    if contract_expiry_after is not None:
        query = query.filter(ExistingScheme.contract_end_date >= contract_expiry_after)
    if min_performance_rating is not None:
        query = query.filter(ExistingScheme.performance_rating >= min_performance_rating)

    sort_col = getattr(ExistingScheme, sort_by)
    if sort_order == "desc":
        sort_col = sort_col.desc().nullslast()
    else:
        sort_col = sort_col.asc().nullsfirst()

    total = query.count()
    items = query.order_by(sort_col).offset(skip).limit(limit).all()

    return SchemeListResponse(items=items, total=total, skip=skip, limit=limit)


@router.get("/expiring", response_model=SchemeListResponse)
def expiring_schemes(
    months: int = Query(12, ge=1, le=60, description="Months until contract expiry"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List schemes with contracts expiring within the given number of months."""
    today = datetime.date.today()
    cutoff = today + datetime.timedelta(days=months * 30)

    query = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.council),
        )
        .filter(
            and_(
                ExistingScheme.contract_end_date.isnot(None),
                ExistingScheme.contract_end_date >= today,
                ExistingScheme.contract_end_date <= cutoff,
            )
        )
    )

    total = query.count()
    items = (
        query.order_by(ExistingScheme.contract_end_date.asc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return SchemeListResponse(items=items, total=total, skip=skip, limit=limit)


@router.get("/contract-timeline")
def contract_timeline(
    months_ahead: int = Query(24, ge=1, le=60),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get all contracts expiring within the given window, grouped by month."""
    today = datetime.date.today()
    cutoff = today + datetime.timedelta(days=months_ahead * 30)

    contracts = (
        db.query(SchemeContract)
        .options(
            joinedload(SchemeContract.scheme),
            joinedload(SchemeContract.operator_company),
        )
        .filter(
            and_(
                SchemeContract.contract_end_date.isnot(None),
                SchemeContract.contract_end_date >= today,
                SchemeContract.contract_end_date <= cutoff,
                SchemeContract.is_current.is_(True),
            )
        )
        .order_by(SchemeContract.contract_end_date.asc())
        .all()
    )

    # Group by month
    timeline = {}
    for c in contracts:
        month_key = c.contract_end_date.strftime("%Y-%m")
        if month_key not in timeline:
            timeline[month_key] = []
        timeline[month_key].append({
            "contract_id": c.id,
            "scheme_id": c.scheme_id,
            "scheme_name": c.scheme.name if c.scheme else None,
            "operator": c.operator_company.name if c.operator_company else None,
            "contract_end_date": str(c.contract_end_date),
            "contract_value": c.contract_value,
        })

    return {"months_ahead": months_ahead, "timeline": timeline, "total_expiring": len(contracts)}


@router.get("/{scheme_id}", response_model=SchemeResponse)
def get_scheme(scheme_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    scheme = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.asset_manager_company),
            joinedload(ExistingScheme.landlord_company),
            joinedload(ExistingScheme.council),
        )
        .filter(ExistingScheme.id == scheme_id)
        .first()
    )
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")
    return scheme


@router.post("", response_model=SchemeResponse, status_code=status.HTTP_201_CREATED)
def create_scheme(data: SchemeCreate, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    scheme = ExistingScheme(**data.model_dump())
    db.add(scheme)
    db.commit()
    db.refresh(scheme)

    scheme = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.council),
        )
        .filter(ExistingScheme.id == scheme.id)
        .first()
    )
    return scheme


@router.put("/{scheme_id}", response_model=SchemeResponse)
def update_scheme(
    scheme_id: int,
    data: SchemeUpdate,
    current_user: User = Depends(require_role("admin", "bd_manager")),
    db: Session = Depends(get_db),
):
    scheme = db.query(ExistingScheme).filter(ExistingScheme.id == scheme_id).first()
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")

    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(scheme, field, value)

    db.commit()

    scheme = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.council),
        )
        .filter(ExistingScheme.id == scheme_id)
        .first()
    )
    return scheme


@router.get("/{scheme_id}/contracts", response_model=list[SchemeContractResponse])
def get_scheme_contracts(scheme_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get full contract history for a scheme, newest first."""
    scheme = db.query(ExistingScheme).filter(ExistingScheme.id == scheme_id).first()
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")
    contracts = (
        db.query(SchemeContract)
        .options(
            joinedload(SchemeContract.operator_company),
            joinedload(SchemeContract.client_company),
        )
        .filter(SchemeContract.scheme_id == scheme_id)
        .order_by(SchemeContract.contract_start_date.desc().nullslast())
        .all()
    )
    return contracts


@router.get("/{scheme_id}/audit-log", response_model=list[SchemeChangeLogResponse])
def get_scheme_audit_log(
    scheme_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")),
    db: Session = Depends(get_db),
):
    """Get change history for a scheme."""
    scheme = db.query(ExistingScheme).filter(ExistingScheme.id == scheme_id).first()
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")
    logs = (
        db.query(SchemeChangeLog)
        .filter(SchemeChangeLog.scheme_id == scheme_id)
        .order_by(SchemeChangeLog.changed_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    return logs


@router.post("/{scheme_id}/refresh", status_code=status.HTTP_202_ACCEPTED)
def trigger_scheme_refresh(scheme_id: int, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    """Queue an on-demand data refresh for a single scheme."""
    scheme = db.query(ExistingScheme).filter(ExistingScheme.id == scheme_id).first()
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")
    from app.tasks.scraping_tasks import refresh_scheme_data
    task = refresh_scheme_data.delay(scheme_id)
    return {"message": "Refresh queued", "task_id": str(task.id), "scheme_id": scheme_id}


@router.delete("/{scheme_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_scheme(scheme_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    scheme = db.query(ExistingScheme).filter(ExistingScheme.id == scheme_id).first()
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")
    db.delete(scheme)
    db.commit()
