import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.models import Company, CompanyAlias, Contact
from app.api.auth import get_current_user, require_role
from app.models.user import User

router = APIRouter(prefix="/api/companies", tags=["Companies"])


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class CompanyAliasResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    alias_name: str
    source: str


class ContactBase(BaseModel):
    full_name: str
    job_title: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    linkedin_url: Optional[str] = None
    source: Optional[str] = None
    confidence_score: Optional[float] = None


class ContactCreate(ContactBase):
    company_id: int


class ContactUpdate(BaseModel):
    full_name: Optional[str] = None
    job_title: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    linkedin_url: Optional[str] = None
    source: Optional[str] = None
    confidence_score: Optional[float] = None
    last_verified_at: Optional[datetime.datetime] = None


class ContactResponse(ContactBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    company_id: int
    last_verified_at: Optional[datetime.datetime] = None
    created_at: datetime.datetime


class CompanyBase(BaseModel):
    name: str
    normalized_name: Optional[str] = None
    companies_house_number: Optional[str] = None
    registered_address: Optional[str] = None
    website: Optional[str] = None
    sic_codes: Optional[dict] = None
    company_type: Optional[str] = None
    parent_company_id: Optional[int] = None
    is_active: bool = True


class CompanyCreate(CompanyBase):
    pass


class CompanyUpdate(BaseModel):
    name: Optional[str] = None
    normalized_name: Optional[str] = None
    companies_house_number: Optional[str] = None
    registered_address: Optional[str] = None
    website: Optional[str] = None
    sic_codes: Optional[dict] = None
    company_type: Optional[str] = None
    parent_company_id: Optional[int] = None
    is_active: Optional[bool] = None


class CompanyResponse(CompanyBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    aliases: list[CompanyAliasResponse] = []
    contacts: list[ContactResponse] = []
    created_at: datetime.datetime
    updated_at: datetime.datetime


class CompanyBriefResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    company_type: Optional[str] = None
    is_active: bool
    companies_house_number: Optional[str] = None


class CompanyListResponse(BaseModel):
    items: list[CompanyBriefResponse]
    total: int
    skip: int
    limit: int


class MergeDuplicatesRequest(BaseModel):
    primary_company_id: int
    duplicate_company_ids: list[int]


class MergeDuplicatesResponse(BaseModel):
    merged_count: int
    primary_company_id: int


def _normalize_name(name: str) -> str:
    """Normalize company name for deduplication."""
    import re
    n = name.upper().strip()
    # Remove common suffixes
    for suffix in [" LIMITED", " LTD", " PLC", " LLP", " INC", " CORP", " LLC"]:
        if n.endswith(suffix):
            n = n[: -len(suffix)]
    n = re.sub(r"[^A-Z0-9 ]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


# ---------------------------------------------------------------------------
# Company endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=CompanyListResponse)
def list_companies(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    search: Optional[str] = None,
    company_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    query = db.query(Company)

    if company_type is not None:
        query = query.filter(Company.company_type == company_type)
    if is_active is not None:
        query = query.filter(Company.is_active == is_active)
    if search is not None:
        pattern = f"%{search}%"
        query = query.filter(
            or_(
                Company.name.ilike(pattern),
                Company.normalized_name.ilike(pattern),
                Company.companies_house_number.ilike(pattern),
            )
        )

    total = query.count()
    items = query.order_by(Company.name).offset(skip).limit(limit).all()

    return CompanyListResponse(items=items, total=total, skip=skip, limit=limit)


@router.get("/{company_id}", response_model=CompanyResponse)
def get_company(company_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    # Strip contact PII for viewers
    if current_user.role == "viewer":
        for contact in company.contacts:
            contact.email = None
            contact.phone = None
            contact.linkedin_url = None
    return company


@router.post("", response_model=CompanyResponse, status_code=status.HTTP_201_CREATED)
def create_company(data: CompanyCreate, current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")), db: Session = Depends(get_db)):
    company_data = data.model_dump()
    if not company_data.get("normalized_name"):
        company_data["normalized_name"] = _normalize_name(company_data["name"])

    company = Company(**company_data)
    db.add(company)
    db.commit()
    db.refresh(company)
    return company


@router.put("/{company_id}", response_model=CompanyResponse)
def update_company(
    company_id: int,
    data: CompanyUpdate,
    current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")),
    db: Session = Depends(get_db),
):
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    update_data = data.model_dump(exclude_unset=True)
    if "name" in update_data and "normalized_name" not in update_data:
        update_data["normalized_name"] = _normalize_name(update_data["name"])
    for field, value in update_data.items():
        setattr(company, field, value)

    db.commit()
    db.refresh(company)
    return company


@router.delete("/{company_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_company(company_id: int, current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    db.delete(company)
    db.commit()


@router.post("/merge", response_model=MergeDuplicatesResponse)
def merge_duplicates(data: MergeDuplicatesRequest, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    """Merge duplicate companies into a single primary company.

    All contacts, aliases, and FK references from duplicate companies are
    re-pointed to the primary company. The duplicates are then deactivated
    and an alias record is created for each duplicate name.
    """
    primary = db.query(Company).filter(Company.id == data.primary_company_id).first()
    if not primary:
        raise HTTPException(status_code=404, detail="Primary company not found")

    duplicates = (
        db.query(Company)
        .filter(Company.id.in_(data.duplicate_company_ids))
        .all()
    )
    if len(duplicates) != len(data.duplicate_company_ids):
        raise HTTPException(status_code=400, detail="One or more duplicate company IDs not found")

    from app.models.models import PlanningApplication, ExistingScheme, PipelineOpportunity

    for dup in duplicates:
        # Move contacts
        db.query(Contact).filter(Contact.company_id == dup.id).update(
            {"company_id": primary.id}, synchronize_session="fetch"
        )

        # Move aliases
        db.query(CompanyAlias).filter(CompanyAlias.company_id == dup.id).update(
            {"company_id": primary.id}, synchronize_session="fetch"
        )

        # Re-point planning application FKs
        db.query(PlanningApplication).filter(
            PlanningApplication.applicant_company_id == dup.id
        ).update({"applicant_company_id": primary.id}, synchronize_session="fetch")
        db.query(PlanningApplication).filter(
            PlanningApplication.agent_company_id == dup.id
        ).update({"agent_company_id": primary.id}, synchronize_session="fetch")

        # Re-point scheme FKs
        db.query(ExistingScheme).filter(
            ExistingScheme.operator_company_id == dup.id
        ).update({"operator_company_id": primary.id}, synchronize_session="fetch")
        db.query(ExistingScheme).filter(
            ExistingScheme.owner_company_id == dup.id
        ).update({"owner_company_id": primary.id}, synchronize_session="fetch")

        # Re-point pipeline FKs
        db.query(PipelineOpportunity).filter(
            PipelineOpportunity.company_id == dup.id
        ).update({"company_id": primary.id}, synchronize_session="fetch")

        # Create alias for the duplicate name
        alias = CompanyAlias(
            company_id=primary.id,
            alias_name=dup.name,
            source="manual",
        )
        db.add(alias)

        # Deactivate duplicate
        dup.is_active = False

    db.commit()
    return MergeDuplicatesResponse(
        merged_count=len(duplicates),
        primary_company_id=primary.id,
    )


# ---------------------------------------------------------------------------
# Contact endpoints
# ---------------------------------------------------------------------------

@router.get("/{company_id}/contacts", response_model=list[ContactResponse])
def list_contacts(company_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company.contacts


@router.post("/contacts", response_model=ContactResponse, status_code=status.HTTP_201_CREATED)
def create_contact(data: ContactCreate, current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")), db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == data.company_id).first()
    if not company:
        raise HTTPException(status_code=400, detail="Company not found")

    contact = Contact(**data.model_dump())
    db.add(contact)
    db.commit()
    db.refresh(contact)
    return contact


@router.put("/contacts/{contact_id}", response_model=ContactResponse)
def update_contact(
    contact_id: int,
    data: ContactUpdate,
    current_user: User = Depends(require_role("admin", "bd_manager", "bd_analyst")),
    db: Session = Depends(get_db),
):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(contact, field, value)

    db.commit()
    db.refresh(contact)
    return contact


@router.delete("/contacts/{contact_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_contact(contact_id: int, current_user: User = Depends(require_role("admin", "bd_manager")), db: Session = Depends(get_db)):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    db.delete(contact)
    db.commit()
