"""
Per-field source precedence tracking for ExistingScheme.

Every write to a protected field MUST go through ``set_field``. The function:
  1. Resolves the source's precedence rank.
  2. Looks up the current lock on this field (scheme.locked_fields[field]).
  3. If the incoming source's rank < current lock's rank → SKIP (return False).
  4. Otherwise: validate, setattr, log to SchemeChangeLog, update locked_fields.

Protected fields (see ``PROTECTED_FIELDS``):
  - num_units
  - operator_company_id, owner_company_id, asset_manager_company_id, landlord_company_id
  - contract_start_date, contract_end_date

Unprotected fields may still use ``set_field`` to get audit logging, but they
never contribute to the lock map.
"""

from __future__ import annotations

import datetime
import re
from typing import Any, Optional

import structlog
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.models import ExistingScheme, SchemeChangeLog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Precedence
# ---------------------------------------------------------------------------

SOURCE_PRECEDENCE: dict[str, int] = {
    "manual": 100,
    "hmlr_ccod": 80,
    "companies_house": 80,
    "arl_btr": 60,
    "arl_btr_open_operating": 60,
    "epc_new_dwelling": 60,
    "epc": 60,
    "operator_scraper": 50,
    "pbsa_operator": 50,
    "find_a_tender": 40,
    "contracts_finder": 40,
    "rsh": 35,
    "ai_enrichment": 20,
    "ai_enrichment_batch": 20,
    "regex_extract": 10,
    "unknown": 10,
}

DEFAULT_PRECEDENCE = 10


def precedence_of(source: str | None) -> int:
    if not source:
        return DEFAULT_PRECEDENCE
    return SOURCE_PRECEDENCE.get(source, DEFAULT_PRECEDENCE)


# ---------------------------------------------------------------------------
# Protected fields & validation
# ---------------------------------------------------------------------------

PROTECTED_FIELDS: set[str] = {
    "num_units",
    "operator_company_id",
    "owner_company_id",
    "asset_manager_company_id",
    "landlord_company_id",
    "contract_start_date",
    "contract_end_date",
}

# Fields that are allowed through set_field (superset of protected — includes
# free-form scalar fields that the manual-edit UI needs to update). Anything
# not listed here is rejected as an unknown write to avoid accidental schema
# drift.
WRITABLE_FIELDS: set[str] = PROTECTED_FIELDS | {
    "name",
    "address",
    "postcode",
    "scheme_type",
    "status",
    "performance_rating",
    "satisfaction_score",
    "regulatory_rating",
    "financial_health_score",
}

_SCHEME_TYPE_ENUM = {"BTR", "PBSA", "Co-living", "Senior Living", "Residential", "Mixed-use"}
_STATUS_ENUM = {"operational", "under_construction", "planned", "decommissioned"}
_POSTCODE_RE = re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s\d[A-Z]{2}$")


class FieldValidationError(ValueError):
    """Raised when a value fails validation for a given field."""


def _validate(field: str, value: Any) -> Any:
    """Return the normalised value or raise FieldValidationError."""
    if value is None:
        return None

    if field == "num_units":
        try:
            n = int(value)
        except (TypeError, ValueError):
            raise FieldValidationError(f"num_units must be an integer, got {value!r}")
        if not (1 <= n <= 5000):
            raise FieldValidationError(
                f"num_units must be between 1 and 5000, got {n}"
            )
        return n

    if field == "postcode":
        pc = str(value).strip().upper()
        # Normalise spacing
        pc = re.sub(r"\s+", " ", pc)
        if " " not in pc and len(pc) >= 5:
            pc = f"{pc[:-3]} {pc[-3:]}"
        if not _POSTCODE_RE.match(pc):
            raise FieldValidationError(f"Invalid UK postcode: {value!r}")
        return pc

    if field == "scheme_type":
        if value not in _SCHEME_TYPE_ENUM:
            raise FieldValidationError(
                f"scheme_type must be one of {_SCHEME_TYPE_ENUM}, got {value!r}"
            )
        return value

    if field == "status":
        if value not in _STATUS_ENUM:
            raise FieldValidationError(
                f"status must be one of {_STATUS_ENUM}, got {value!r}"
            )
        return value

    if field in ("contract_start_date", "contract_end_date"):
        if isinstance(value, datetime.date):
            return value
        if isinstance(value, str):
            try:
                return datetime.date.fromisoformat(value[:10])
            except ValueError:
                raise FieldValidationError(
                    f"{field} must be ISO date YYYY-MM-DD, got {value!r}"
                )
        raise FieldValidationError(f"{field} must be a date, got {type(value).__name__}")

    if field.endswith("_company_id"):
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            raise FieldValidationError(f"{field} must be an integer, got {value!r}")

    if field in ("performance_rating", "satisfaction_score", "financial_health_score"):
        try:
            f = float(value)
        except (TypeError, ValueError):
            raise FieldValidationError(f"{field} must be a float, got {value!r}")
        if not (0 <= f <= 100):
            raise FieldValidationError(f"{field} must be between 0 and 100, got {f}")
        return f

    # Free-form strings
    return value


# ---------------------------------------------------------------------------
# Core write chokepoint
# ---------------------------------------------------------------------------

def can_write(
    scheme: ExistingScheme, field: str, source: str
) -> bool:
    """Return True if ``source`` is allowed to overwrite ``field`` on ``scheme``.

    Only protected fields consult the lock map; other writable fields always
    allow writes (they still go through audit logging).
    """
    if field not in PROTECTED_FIELDS:
        return True
    locks = scheme.locked_fields or {}
    current_source = locks.get(field)
    if current_source is None:
        return True
    return precedence_of(source) >= precedence_of(current_source)


def set_field(
    scheme: ExistingScheme,
    field: str,
    value: Any,
    source: str,
    db: Session,
    *,
    changed_by: str = "system",
) -> bool:
    """Write ``value`` to ``scheme.field`` if precedence allows.

    Returns True if applied, False if skipped due to lock precedence or no-op.
    Raises FieldValidationError on invalid value.

    Always goes through SchemeChangeLog for audit; always updates locked_fields
    when the write is accepted.
    """
    if field not in WRITABLE_FIELDS:
        raise FieldValidationError(f"Field not writable: {field!r}")

    if not can_write(scheme, field, source):
        logger.debug(
            "set_field_blocked",
            scheme_id=scheme.id,
            field=field,
            source=source,
            current_lock=(scheme.locked_fields or {}).get(field),
        )
        return False

    normalised = _validate(field, value)

    old_value = getattr(scheme, field, None)
    # No-op: same value, same source — still record source refresh but don't
    # spam change log. Skip silently when value truly unchanged.
    if old_value == normalised:
        return False

    setattr(scheme, field, normalised)

    # Update lock map (only for protected fields)
    if field in PROTECTED_FIELDS:
        locks = dict(scheme.locked_fields or {})
        locks[field] = source
        scheme.locked_fields = locks
        flag_modified(scheme, "locked_fields")

    # Audit log
    db.add(SchemeChangeLog(
        scheme_id=scheme.id,
        field_name=field,
        old_value=str(old_value) if old_value is not None else None,
        new_value=str(normalised) if normalised is not None else None,
        source=source,
        changed_by=changed_by,
    ))

    logger.info(
        "set_field_applied",
        scheme_id=scheme.id,
        field=field,
        source=source,
        old=str(old_value)[:60] if old_value is not None else None,
        new=str(normalised)[:60] if normalised is not None else None,
    )
    return True


# ---------------------------------------------------------------------------
# Data confidence scoring
# ---------------------------------------------------------------------------

_TRUSTED_LOCK_SOURCES = {
    "manual",
    "hmlr_ccod",
    "companies_house",
    "arl_btr",
    "arl_btr_open_operating",
    "epc_new_dwelling",
    "epc",
    "operator_scraper",
    "pbsa_operator",
}


def compute_data_confidence(scheme: ExistingScheme) -> float:
    """Compute a 0-1 confidence score based on how many protected fields
    are populated and locked by trusted sources.
    """
    locks = scheme.locked_fields or {}
    score = 0.0
    per_field = 1.0 / len(PROTECTED_FIELDS)

    for field in PROTECTED_FIELDS:
        value = getattr(scheme, field, None)
        if value is None:
            continue
        lock_source = locks.get(field)
        if lock_source == "manual":
            score += per_field
        elif lock_source in _TRUSTED_LOCK_SOURCES:
            score += per_field * 0.8
        else:
            score += per_field * 0.4

    return round(min(1.0, score), 3)
