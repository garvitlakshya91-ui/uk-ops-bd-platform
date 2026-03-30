"""Central RBAC permissions module.

Defines the permission matrix, stage-gating rules, PII stripping helpers,
and reusable FastAPI dependencies for ownership checks.
"""

from typing import Optional

from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.api.auth import get_current_user
from app.models.user import User
from app.models.models import PipelineOpportunity


# ---------------------------------------------------------------------------
# Role-based alert type access
# ---------------------------------------------------------------------------

ALERT_TYPES_BY_ROLE: dict[str, Optional[list[str]]] = {
    "admin": None,  # all types
    "bd_manager": None,  # all types
    "bd_analyst": ["new_application", "new_opportunity", "status_change"],
    "viewer": ["contract_expiring"],
}


# ---------------------------------------------------------------------------
# Pipeline stages analysts can set
# ---------------------------------------------------------------------------

ANALYST_MAX_STAGES = {"identified", "researched", "contacted"}


# ---------------------------------------------------------------------------
# Permission helpers
# ---------------------------------------------------------------------------

def can_edit_pipeline(user: User, opportunity: PipelineOpportunity) -> bool:
    """Return True if *user* is allowed to edit *opportunity*.

    Admins and BD managers can edit any record.  Analysts can only edit
    records assigned to them (via ``assigned_to_user_id``).
    """
    if user.role in ("admin", "bd_manager"):
        return True
    if user.role == "bd_analyst":
        return opportunity.assigned_to_user_id == user.id
    return False


def require_pipeline_ownership_or_role(*allowed_roles: str):
    """Return a FastAPI dependency that verifies the current user either:

    * has one of *allowed_roles*, **or**
    * is a ``bd_analyst`` who owns the pipeline record (by path param
      ``opportunity_id``).

    The dependency returns a ``(User, PipelineOpportunity)`` tuple so
    callers don't need to re-query.
    """

    def checker(
        opportunity_id: int,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> tuple[User, PipelineOpportunity]:
        opp = (
            db.query(PipelineOpportunity)
            .filter(PipelineOpportunity.id == opportunity_id)
            .first()
        )
        if not opp:
            raise HTTPException(status_code=404, detail="Opportunity not found")

        if current_user.role in allowed_roles:
            return current_user, opp

        if current_user.role == "bd_analyst" and opp.assigned_to_user_id == current_user.id:
            return current_user, opp

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to modify this pipeline record.",
        )

    return checker


# ---------------------------------------------------------------------------
# Stage gating
# ---------------------------------------------------------------------------

def check_analyst_stage_gating(user: User, new_stage: str) -> None:
    """Raise 403 if an analyst tries to set a stage beyond the allowed set."""
    if user.role == "bd_analyst" and new_stage not in ANALYST_MAX_STAGES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Analysts can only advance opportunities to: {', '.join(sorted(ANALYST_MAX_STAGES))}",
        )


# ---------------------------------------------------------------------------
# Contact PII stripping for viewers
# ---------------------------------------------------------------------------

def strip_contact_pii(contact_data: dict) -> dict:
    """Remove PII fields from a contact dict for viewers."""
    stripped = dict(contact_data)
    for field in ("email", "phone", "linkedin_url"):
        if field in stripped:
            stripped[field] = None
    return stripped


def strip_contacts_from_company(company_dict: dict) -> dict:
    """Strip PII from all contacts within a company response dict."""
    if "contacts" in company_dict and company_dict["contacts"]:
        company_dict["contacts"] = [
            strip_contact_pii(c) if isinstance(c, dict) else c
            for c in company_dict["contacts"]
        ]
    return company_dict


# ---------------------------------------------------------------------------
# Alert type filtering
# ---------------------------------------------------------------------------

def get_allowed_alert_types(user: User) -> Optional[list[str]]:
    """Return the list of alert types the user can see, or None for all."""
    return ALERT_TYPES_BY_ROLE.get(user.role)
