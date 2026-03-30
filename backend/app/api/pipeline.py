import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.models.models import PipelineOpportunity, Company, Contact, PlanningApplication, ExistingScheme
from app.api.auth import get_current_user, require_role
from app.models.user import User
from app.api.permissions import can_edit_pipeline, check_analyst_stage_gating

router = APIRouter(prefix="/api/pipeline", tags=["Pipeline"])

VALID_STAGES = ["identified", "researched", "contacted", "meeting", "proposal", "won", "lost"]
VALID_PRIORITIES = ["hot", "warm", "cold"]


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class OpportunityBase(BaseModel):
    source: str
    planning_application_id: Optional[int] = None
    scheme_id: Optional[int] = None
    company_id: int
    stage: str = "identified"
    priority: str = "warm"
    bd_score: Optional[float] = None
    assigned_to: Optional[str] = None
    last_contact_date: Optional[datetime.date] = None
    next_action: Optional[str] = None
    next_action_date: Optional[datetime.date] = None
    notes: Optional[str] = None


class OpportunityCreate(OpportunityBase):
    pass


class OpportunityUpdate(BaseModel):
    stage: Optional[str] = None
    priority: Optional[str] = None
    bd_score: Optional[float] = None
    assigned_to: Optional[str] = None
    last_contact_date: Optional[datetime.date] = None
    next_action: Optional[str] = None
    next_action_date: Optional[datetime.date] = None
    notes: Optional[str] = None


class BulkStageUpdate(BaseModel):
    opportunity_ids: list[int]
    stage: str


class CompanyBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    company_type: Optional[str] = None


class ApplicationBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    reference: str
    address: Optional[str] = None
    scheme_type: str


class SchemeBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    scheme_type: Optional[str] = None


class OpportunityResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    source: str
    planning_application_id: Optional[int] = None
    scheme_id: Optional[int] = None
    company_id: int
    stage: str
    priority: str
    bd_score: Optional[float] = None
    assigned_to: Optional[str] = None
    last_contact_date: Optional[datetime.date] = None
    next_action: Optional[str] = None
    next_action_date: Optional[datetime.date] = None
    notes: Optional[str] = None
    company: Optional[CompanyBrief] = None
    planning_application: Optional[ApplicationBrief] = None
    scheme: Optional[SchemeBrief] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime


class OpportunityListResponse(BaseModel):
    items: list[OpportunityResponse]
    total: int
    skip: int
    limit: int


class StageCount(BaseModel):
    stage: str
    count: int
    total_bd_score: Optional[float] = None


class PriorityCount(BaseModel):
    priority: str
    count: int


class PipelineStats(BaseModel):
    total: int
    by_stage: list[StageCount]
    by_priority: list[PriorityCount]


class KanbanColumn(BaseModel):
    stage: str
    items: list[OpportunityResponse]
    count: int


class KanbanBoard(BaseModel):
    columns: list[KanbanColumn]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=OpportunityListResponse)
def list_opportunities(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    stage: Optional[str] = None,
    priority: Optional[str] = None,
    source: Optional[str] = None,
    assigned_to: Optional[str] = None,
    company_id: Optional[int] = None,
    sort_by: str = Query("created_at", pattern="^(created_at|bd_score|next_action_date|updated_at)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    query = db.query(PipelineOpportunity).options(
        joinedload(PipelineOpportunity.company),
        joinedload(PipelineOpportunity.planning_application),
        joinedload(PipelineOpportunity.scheme),
    )

    # Auto-filter for analysts: only see their own records
    if current_user.role == "bd_analyst":
        query = query.filter(PipelineOpportunity.assigned_to_user_id == current_user.id)

    if stage is not None:
        query = query.filter(PipelineOpportunity.stage == stage)
    if priority is not None:
        query = query.filter(PipelineOpportunity.priority == priority)
    if source is not None:
        query = query.filter(PipelineOpportunity.source == source)
    if assigned_to is not None:
        query = query.filter(PipelineOpportunity.assigned_to == assigned_to)
    if company_id is not None:
        query = query.filter(PipelineOpportunity.company_id == company_id)

    sort_col = getattr(PipelineOpportunity, sort_by)
    if sort_order == "desc":
        sort_col = sort_col.desc().nullslast()
    else:
        sort_col = sort_col.asc().nullsfirst()

    total = query.count()
    items = query.order_by(sort_col).offset(skip).limit(limit).all()

    return OpportunityListResponse(items=items, total=total, skip=skip, limit=limit)


@router.get("/stats", response_model=PipelineStats)
def pipeline_stats(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    total = db.query(func.count(PipelineOpportunity.id)).scalar() or 0

    by_stage = [
        StageCount(stage=row[0], count=row[1], total_bd_score=row[2])
        for row in db.query(
            PipelineOpportunity.stage,
            func.count(PipelineOpportunity.id),
            func.sum(PipelineOpportunity.bd_score),
        )
        .group_by(PipelineOpportunity.stage)
        .all()
    ]

    by_priority = [
        PriorityCount(priority=row[0], count=row[1])
        for row in db.query(
            PipelineOpportunity.priority,
            func.count(PipelineOpportunity.id),
        )
        .group_by(PipelineOpportunity.priority)
        .all()
    ]

    return PipelineStats(total=total, by_stage=by_stage, by_priority=by_priority)


@router.get("/kanban", response_model=KanbanBoard)
def kanban_board(
    assigned_to: Optional[str] = None,
    priority: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return pipeline data structured as a kanban board with one column per stage."""
    columns: list[KanbanColumn] = []
    for stage in VALID_STAGES:
        query = db.query(PipelineOpportunity).options(
            joinedload(PipelineOpportunity.company),
            joinedload(PipelineOpportunity.planning_application),
            joinedload(PipelineOpportunity.scheme),
        ).filter(PipelineOpportunity.stage == stage)

        # Auto-filter for analysts: only see their own records
        if current_user.role == "bd_analyst":
            query = query.filter(PipelineOpportunity.assigned_to_user_id == current_user.id)

        if assigned_to is not None:
            query = query.filter(PipelineOpportunity.assigned_to == assigned_to)
        if priority is not None:
            query = query.filter(PipelineOpportunity.priority == priority)

        items = query.order_by(
            PipelineOpportunity.bd_score.desc().nullslast()
        ).limit(100).all()

        columns.append(KanbanColumn(stage=stage, items=items, count=len(items)))

    return KanbanBoard(columns=columns)


@router.get("/{opportunity_id}", response_model=OpportunityResponse)
def get_opportunity(opportunity_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    opp = (
        db.query(PipelineOpportunity)
        .options(
            joinedload(PipelineOpportunity.company),
            joinedload(PipelineOpportunity.planning_application),
            joinedload(PipelineOpportunity.scheme),
        )
        .filter(PipelineOpportunity.id == opportunity_id)
        .first()
    )
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    # 403 if analyst and not assigned to this record
    if current_user.role == "bd_analyst" and opp.assigned_to_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="You do not have permission to view this opportunity")
    return opp


@router.post("", response_model=OpportunityResponse, status_code=status.HTTP_201_CREATED)
def create_opportunity(data: OpportunityCreate, current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")), db: Session = Depends(get_db)):
    if data.stage not in VALID_STAGES:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {VALID_STAGES}")
    if data.priority not in VALID_PRIORITIES:
        raise HTTPException(status_code=400, detail=f"Invalid priority. Must be one of: {VALID_PRIORITIES}")

    company = db.query(Company).filter(Company.id == data.company_id).first()
    if not company:
        raise HTTPException(status_code=400, detail="Company not found")

    # Prevent duplicate pipeline entries for the same scheme
    if data.scheme_id is not None:
        existing = db.query(PipelineOpportunity).filter(
            PipelineOpportunity.scheme_id == data.scheme_id
        ).first()
        if existing:
            raise HTTPException(status_code=409, detail="A pipeline opportunity already exists for this scheme")

    # Stage gating for analysts
    check_analyst_stage_gating(current_user, data.stage)

    opp = PipelineOpportunity(**data.model_dump())

    # Auto-set assigned_to_user_id for analysts
    if current_user.role == "bd_analyst":
        opp.assigned_to_user_id = current_user.id

    db.add(opp)
    db.commit()
    db.refresh(opp)

    # Trigger contact enrichment if company has no contacts
    contact_count = db.query(func.count(Contact.id)).filter(
        Contact.company_id == data.company_id
    ).scalar() or 0
    if contact_count == 0:
        try:
            from app.tasks.enrichment_tasks import enrich_company
            enrich_company.delay(data.company_id)
        except Exception:
            pass  # Don't fail pipeline creation if enrichment can't be queued

    # Re-query with joins
    opp = (
        db.query(PipelineOpportunity)
        .options(
            joinedload(PipelineOpportunity.company),
            joinedload(PipelineOpportunity.planning_application),
            joinedload(PipelineOpportunity.scheme),
        )
        .filter(PipelineOpportunity.id == opp.id)
        .first()
    )
    return opp


@router.put("/{opportunity_id}", response_model=OpportunityResponse)
def update_opportunity(
    opportunity_id: int,
    data: OpportunityUpdate,
    current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")),
    db: Session = Depends(get_db),
):
    opp = db.query(PipelineOpportunity).filter(PipelineOpportunity.id == opportunity_id).first()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    # Analyst can only edit own records
    if not can_edit_pipeline(current_user, opp):
        raise HTTPException(status_code=403, detail="You do not have permission to edit this opportunity")

    update_data = data.model_dump(exclude_unset=True)

    # Stage gating for analysts
    if "stage" in update_data:
        check_analyst_stage_gating(current_user, update_data["stage"])

    if "stage" in update_data and update_data["stage"] not in VALID_STAGES:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {VALID_STAGES}")
    if "priority" in update_data and update_data["priority"] not in VALID_PRIORITIES:
        raise HTTPException(status_code=400, detail=f"Invalid priority. Must be one of: {VALID_PRIORITIES}")

    for field, value in update_data.items():
        setattr(opp, field, value)

    db.commit()

    opp = (
        db.query(PipelineOpportunity)
        .options(
            joinedload(PipelineOpportunity.company),
            joinedload(PipelineOpportunity.planning_application),
            joinedload(PipelineOpportunity.scheme),
        )
        .filter(PipelineOpportunity.id == opportunity_id)
        .first()
    )
    return opp


@router.put("/{opportunity_id}/stage", response_model=OpportunityResponse)
def update_stage(
    opportunity_id: int,
    stage: str = Query(...),
    current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")),
    db: Session = Depends(get_db),
):
    if stage not in VALID_STAGES:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {VALID_STAGES}")

    opp = db.query(PipelineOpportunity).filter(PipelineOpportunity.id == opportunity_id).first()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    # Analyst can only edit own records
    if not can_edit_pipeline(current_user, opp):
        raise HTTPException(status_code=403, detail="You do not have permission to edit this opportunity")

    # Stage gating for analysts
    check_analyst_stage_gating(current_user, stage)

    opp.stage = stage
    db.commit()

    opp = (
        db.query(PipelineOpportunity)
        .options(
            joinedload(PipelineOpportunity.company),
            joinedload(PipelineOpportunity.planning_application),
            joinedload(PipelineOpportunity.scheme),
        )
        .filter(PipelineOpportunity.id == opportunity_id)
        .first()
    )
    return opp


@router.post("/bulk-update-stage", response_model=dict)
def bulk_update_stage(data: BulkStageUpdate, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    if data.stage not in VALID_STAGES:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {VALID_STAGES}")

    updated = (
        db.query(PipelineOpportunity)
        .filter(PipelineOpportunity.id.in_(data.opportunity_ids))
        .update({"stage": data.stage}, synchronize_session="fetch")
    )
    db.commit()
    return {"updated_count": updated, "stage": data.stage}


@router.delete("/{opportunity_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_opportunity(opportunity_id: int, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    opp = db.query(PipelineOpportunity).filter(PipelineOpportunity.id == opportunity_id).first()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    db.delete(opp)
    db.commit()
