"""AI-powered scheme enrichment using Claude.

Provides endpoints for enriching existing scheme data by querying an LLM
about UK property developments. Suggestions are returned for user review
before being applied.
"""

import json
from typing import Any, Optional

import anthropic
import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session, joinedload

from app.config import settings
from app.database import get_db
from app.models.models import ExistingScheme, Company, SchemeChangeLog
from app.api.auth import get_current_user, require_role
from app.models.user import User

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/schemes", tags=["AI Enrichment"])

AI_MODEL = "claude-sonnet-4-20250514"

# Anthropic server-side web search tool
WEB_SEARCH_TOOL = {
    "type": "web_search_20250305",
    "name": "web_search",
    "max_uses": 5,
}


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class AIFieldSuggestion(BaseModel):
    """A single AI-suggested value for a scheme field."""
    field: str
    current_value: Optional[str] = None
    suggested_value: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0, description="0.0-1.0 confidence score")
    reasoning: Optional[str] = None


class AIEnrichmentResponse(BaseModel):
    """Full AI enrichment result for a single scheme."""
    scheme_id: int
    scheme_name: str
    suggestions: list[AIFieldSuggestion]
    rents: list["AIRentSuggestion"] = []
    model_used: str
    raw_ai_notes: Optional[str] = None
    web_search_used: bool = False


class ApplySuggestionItem(BaseModel):
    """A single field the user has approved for update."""
    field: str
    value: Optional[str] = None


class AIRentSuggestion(BaseModel):
    """A single AI-suggested rent tier for a scheme."""
    room_type: Optional[str] = None
    rent_per_week: Optional[float] = None
    rent_per_month: Optional[float] = None
    currency: Optional[str] = "GBP"
    academic_year: Optional[str] = None
    contract_length_weeks: Optional[int] = None
    confidence: float = 0.0
    reasoning: Optional[str] = None


class ApplySuggestionsRequest(BaseModel):
    """Request body for applying approved AI suggestions."""
    suggestions: list[ApplySuggestionItem]
    rents: list[AIRentSuggestion] = []


class BatchEnrichRequest(BaseModel):
    """Request body for batch AI enrichment."""
    scheme_ids: list[int] = Field(..., min_length=1, max_length=50)


class BatchEnrichResponse(BaseModel):
    """Response for batch AI enrichment."""
    results: list[AIEnrichmentResponse]
    total_requested: int
    total_enriched: int
    errors: list[dict] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_scheme_context(scheme: ExistingScheme, db: Optional["Session"] = None) -> str:
    """Build a textual description of a scheme's known data for the AI prompt.

    When ``db`` is provided, also injects:
      - HMLR CCOD freehold owner at the postcode (if any)
      - Companies House data for the linked operator (if any)
      - Excerpt of the operator's source_reference page (if it's a URL)
    """
    lines = [
        f"Scheme Name: {scheme.name}",
        f"Address: {scheme.address or 'Unknown'}",
        f"Postcode: {scheme.postcode or 'Unknown'}",
    ]
    if scheme.council:
        lines.append(f"Council/Local Authority: {scheme.council.name} ({scheme.council.region or 'region unknown'})")
    else:
        lines.append("Council/Local Authority: Unknown")
    if scheme.operator_company:
        op = scheme.operator_company
        lines.append(f"Current Operator: {op.name}")
        ch = getattr(op, "companies_house_number", None)
        addr = getattr(op, "registered_address", None)
        if ch or addr:
            parts = []
            if ch:
                parts.append(f"CH no. {ch}")
            if addr:
                parts.append(f"registered at {addr}")
            lines.append(f"  Operator Companies House: {'; '.join(parts)}")
    else:
        lines.append("Current Operator: Unknown")
    if scheme.owner_company:
        lines.append(f"Current Owner/Developer: {scheme.owner_company.name}")
    else:
        lines.append("Current Owner/Developer: Unknown")
    if scheme.asset_manager_company:
        lines.append(f"Asset Manager: {scheme.asset_manager_company.name}")
    else:
        lines.append("Asset Manager: Unknown")
    if scheme.landlord_company:
        lines.append(f"Landlord: {scheme.landlord_company.name}")
    else:
        lines.append("Landlord: Unknown")
    lines.append(f"Number of Units: {scheme.num_units or 'Unknown'}")
    lines.append(f"Scheme Type: {scheme.scheme_type or 'Unknown'}")
    lines.append(f"Status: {scheme.status or 'Unknown'}")
    lines.append(f"Source: {scheme.source or 'Unknown'}")
    lines.append(f"HMLR Tenure: {scheme.hmlr_tenure or 'Unknown'}")
    if scheme.epc_ratings:
        lines.append(f"EPC Ratings Data: {json.dumps(scheme.epc_ratings)}")

    # Extra grounding: HMLR freehold owners found at this postcode
    if db is not None and scheme.postcode:
        try:
            ccod_owners = _lookup_postcode_freeholders(scheme.postcode, scheme.id, db)
            if ccod_owners:
                lines.append("")
                lines.append("HMLR freehold owners at this postcode (other schemes):")
                for owner in ccod_owners[:5]:
                    lines.append(f"  - {owner}")
        except Exception:
            pass  # best-effort only

    # Scrape the operator's own page for this scheme if we have a URL
    if scheme.source_reference and scheme.source_reference.startswith(("http://", "https://")):
        try:
            excerpt = _fetch_operator_page_excerpt(scheme.source_reference)
            if excerpt:
                lines.append("")
                lines.append(f"Excerpt from operator's page ({scheme.source_reference}):")
                lines.append(excerpt)
        except Exception:
            pass

    return "\n".join(lines)


def _lookup_postcode_freeholders(postcode: str, exclude_scheme_id: int, db) -> list[str]:
    """Return distinct freehold owner-company names already matched by HMLR
    CCOD for this postcode. Gives the AI a clue about likely ownership."""
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT DISTINCT c.name
        FROM existing_schemes s
        JOIN companies c ON c.id = s.owner_company_id
        WHERE s.postcode = :pc
          AND s.owner_company_id IS NOT NULL
          AND s.id != :sid
          AND s.hmlr_title_number IS NOT NULL
        LIMIT 10
    """), {"pc": postcode, "sid": exclude_scheme_id}).fetchall()
    return [r[0] for r in rows if r and r[0]]


def _fetch_operator_page_excerpt(url: str, max_chars: int = 3000) -> Optional[str]:
    """Fetch an HTML page, strip tags/scripts, return a short excerpt."""
    import re
    import httpx
    try:
        with httpx.Client(
            timeout=10.0, follow_redirects=True, verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; UKOpsBD/1.0)",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            },
        ) as client:
            resp = client.get(url)
            if resp.status_code >= 400:
                return None
            html = resp.text
    except Exception:
        return None

    # Strip scripts/styles, then tags, then collapse whitespace
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&pound;|&#163;", "£", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars] if text else None


SYSTEM_PROMPT = """\
You are an expert on UK property developments, specifically Build-to-Rent (BTR), \
Purpose-Built Student Accommodation (PBSA), Co-living, Senior Living, and \
large-scale residential schemes.

You will be given details about a property development scheme in the UK. \
Some fields may be missing or incomplete (marked as "Unknown"). Suggest values \
ONLY for fields you are genuinely confident about. Being accurate matters much \
more than being comprehensive.

CALIBRATION RULES (critical):
- Confidence 0.8-1.0: You have specific, verifiable knowledge of THIS scheme.
- Confidence 0.5-0.8: Strong inference from closely related facts you know.
- Confidence 0.3-0.5: Reasonable guess from context.
- Confidence below 0.3: Speculation — the user will reject these, so only include if you have a reason to guess.
- Never default owner/asset_manager/landlord to the operator. PBSA and BTR schemes frequently have separate investor/landlord entities distinct from the operator. If you do not specifically know the ownership structure of THIS scheme, either omit these fields or return confidence below 0.3.
- Never invent unit counts. If you don't know the specific scheme's unit count, OMIT num_units rather than guessing from building size.
- For address, only suggest if you know the actual street address of THIS scheme; do not generalise from the city.
- Omit fields entirely if you have no basis to suggest a value.

FORMAT RULES:
- For scheme_type, use exactly one of: BTR, PBSA, Co-living, Senior Living, Residential, Mixed-use.
- For status, use exactly one of: operational, under_construction, planned, decommissioned.
- For company names, use the commonly known trading name.
- For num_units, integer only.

Respond with ONLY valid JSON (no markdown, no code fences):
{
  "suggestions": [
    {
      "field": "field_name",
      "suggested_value": "value",
      "confidence": 0.85,
      "reasoning": "Brief explanation including your source of knowledge"
    }
  ],
  "rents": [
    {
      "room_type": "Studio",
      "rent_per_week": 235,
      "currency": "GBP",
      "academic_year": "2025/26",
      "confidence": 0.9,
      "reasoning": "Published on operator website as Standard Studio"
    }
  ],
  "notes": "Any additional context"
}

Fields you can suggest in "suggestions":
- owner_company_name, operator_company_name, asset_manager_company_name, landlord_company_name
- num_units, scheme_type, status, address, postcode

RENTS (optional array, can be empty):
- One entry per room type / tenancy variant.
- For PBSA, common room types are: Studio, Classic Studio, Deluxe Studio, Premium Studio, Ensuite, Standard Ensuite, Silver Ensuite, Gold Ensuite, 1-bed Apartment, 2-bed Apartment, Cluster Room.
- For BTR, use: Studio, 1-bed, 2-bed, 3-bed.
- Use rent_per_week for PBSA (quoted as £X per week) and rent_per_month for BTR (quoted as £X pcm). Fill only the one you are certain about.
- Include academic_year for PBSA (e.g. "2025/26"); omit for BTR.
- Only include rents you found on the operator's own website, listing sites (Rightmove/Zoopla/StuRents), or official sources via web search. Never guess.
"""


EXTRACTION_SYSTEM_PROMPT = """\
You are a structured data extractor for UK property schemes. You will be given \
details about one scheme plus THREE grounding sources:
  - "HMLR freehold owners at this postcode (other schemes)": a short list of \
    freeholder company names appearing in HM Land Registry's CCOD dataset \
    for other schemes at the same postcode.
  - "Operator Companies House": the linked operator's Companies House number \
    and registered address.
  - "Excerpt from operator's page": raw text scraped from the operator's own \
    scheme page.

CRITICAL RULES:
- You MUST extract values ONLY from the three grounding sources above.
- DO NOT use your general knowledge about this scheme, this operator, or UK \
  property in general. If the value is not visible in the provided context, \
  OMIT the suggestion entirely.
- DO NOT infer owner = operator. PBSA and BTR schemes almost always have a \
  separate investor / landlord entity distinct from the operator. If the \
  context does not explicitly name an owner, asset manager, or landlord, \
  OMIT those fields. Do not default them to the operator.
- DO NOT estimate, round, or guess rents. Only return a rent entry if the \
  operator page excerpt contains an explicit "£X per week", "£X pcm", or \
  equivalent figure for a specific room type.
- DO NOT guess addresses or postcodes from the scheme name or city. Only \
  extract an address that literally appears in the operator page excerpt.
- DO NOT guess unit counts. Only return num_units if the operator page \
  excerpt contains a phrase like "123 bedrooms", "123 studios", "home to 123 \
  students", or similar explicit figure.
- If none of the grounding sources mention a field, return an empty \
  "suggestions" array. Empty responses are GOOD — they mean no data was \
  hallucinated.

CONFIDENCE CALIBRATION:
- 0.9-1.0: The exact value is quoted verbatim in the grounding source.
- 0.7-0.9: The value is clearly implied by explicit text in the grounding \
  source (e.g. room-type list with prices).
- Below 0.7: Do not suggest. Omit instead.

FORMAT RULES:
- scheme_type: BTR, PBSA, Co-living, Senior Living, Residential, Mixed-use.
- status: operational, under_construction, planned, decommissioned.
- Company names: the trading name as it appears in the source.
- num_units: integer only.
- Response: ONLY valid JSON (no markdown, no code fences):

{
  "suggestions": [
    {
      "field": "field_name",
      "suggested_value": "value",
      "confidence": 0.85,
      "reasoning": "Quoted phrase from <source name> where this appears"
    }
  ],
  "rents": [
    {
      "room_type": "Studio",
      "rent_per_week": 235,
      "currency": "GBP",
      "academic_year": "2025/26",
      "confidence": 0.9,
      "reasoning": "Quoted from operator page excerpt"
    }
  ],
  "notes": "Short note about what the grounding sources did / did not provide."
}

Fields you can suggest in "suggestions":
- owner_company_name, operator_company_name, asset_manager_company_name, landlord_company_name
- num_units, scheme_type, status, address, postcode
"""


def _get_anthropic_client() -> anthropic.Anthropic:
    """Create an Anthropic client, raising a clear error if no API key is set."""
    import os
    from pathlib import Path
    api_key = settings.ANTHROPIC_API_KEY or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        # Last resort: load directly from .env file
        env_path = Path(__file__).resolve().parent.parent.parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    api_key = line.split("=", 1)[1].strip()
                    break
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ANTHROPIC_API_KEY is not configured. AI enrichment is unavailable.",
        )
    return anthropic.Anthropic(api_key=api_key)


def _call_claude(
    scheme: ExistingScheme,
    db: Optional[Session] = None,
    use_web_search: bool = True,
    prompt_variant: str = "knowledge",
) -> dict:
    """Call Claude to get enrichment suggestions for a scheme.

    Parameters
    ----------
    use_web_search : bool
        When True, Claude uses the Anthropic web-search tool. When False,
        Claude only has the grounding context we inject.
    prompt_variant : str
        "knowledge" (default) — the original prompt allowing Claude to use
        its internal knowledge.
        "extract" — extraction-only prompt that tells Claude to parse values
        strictly out of the provided grounding sources (CCOD / CH / operator
        page excerpt) and omit anything not found in them.
    """
    client = _get_anthropic_client()
    scheme_context = _build_scheme_context(scheme, db=db)
    system_prompt = (
        EXTRACTION_SYSTEM_PROMPT if prompt_variant == "extract" else SYSTEM_PROMPT
    )

    if use_web_search:
        user_message = (
            "Research the following UK property development scheme using web "
            "search to find accurate, authoritative information about its "
            "ownership, asset manager, unit count, and rents. Then return your "
            "findings as JSON per the system prompt format.\n\n"
            "Search queries to try: the scheme name with the operator name, "
            "the scheme name with 'Companies House' or 'asset manager', the "
            "scheme's postcode with 'freehold', and the operator's website "
            "for the specific property page.\n\n"
            f"Scheme details:\n{scheme_context}"
        )
    elif prompt_variant == "extract":
        user_message = (
            "Extract structured data ONLY from the grounding sources below "
            "(HMLR owners, Companies House, operator page excerpt). Return an "
            "empty suggestions array if the context does not explicitly mention "
            "the field — do NOT use general knowledge.\n\n"
            f"{scheme_context}"
        )
    else:
        user_message = (
            "Please analyse the following UK property development scheme and "
            f"suggest values for any missing or incomplete fields:\n\n{scheme_context}"
        )

    logger.info(
        "ai_enrichment_request",
        scheme_id=scheme.id, scheme_name=scheme.name,
        use_web_search=use_web_search,
    )

    create_kwargs: dict = {
        "model": AI_MODEL,
        "max_tokens": 4096 if use_web_search else 1024,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}],
    }
    if use_web_search:
        create_kwargs["tools"] = [WEB_SEARCH_TOOL]

    message = client.messages.create(**create_kwargs)

    # Web search adds tool_use + tool_result blocks; the final text block is
    # the model's answer. Concatenate all text blocks (usually one).
    text_parts: list[str] = []
    for block in message.content:
        block_type = getattr(block, "type", None)
        if block_type == "text":
            text_parts.append(getattr(block, "text", ""))
    raw_text = "".join(text_parts).strip()

    if not raw_text:
        logger.error("ai_enrichment_empty_response", scheme_id=scheme.id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="AI returned no text answer.",
        )

    # Extract the first JSON object from the response (web-search responses
    # may wrap the JSON in prose or markdown fences even when asked not to).
    json_text = _extract_json_block(raw_text)

    try:
        result = json.loads(json_text)
    except json.JSONDecodeError:
        logger.error(
            "ai_enrichment_parse_error",
            scheme_id=scheme.id, raw=raw_text[:500],
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to parse AI response. The model returned invalid JSON.",
        )

    return result


def _extract_json_block(text: str) -> str:
    """Pull out the first valid JSON object from a string. Handles markdown
    code fences and stray prose around the JSON."""
    import re
    # Strip ```json ... ``` fences
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fence:
        return fence.group(1)
    # Fall back: find the first {...} balanced block
    start = text.find("{")
    if start == -1:
        return text
    depth = 0
    for i in range(start, len(text)):
        c = text[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return text[start:]


def _build_rent_suggestions(ai_result: dict) -> list[AIRentSuggestion]:
    """Extract and normalise rent suggestions from the AI response."""
    out: list[AIRentSuggestion] = []
    for r in ai_result.get("rents", []) or []:
        if not isinstance(r, dict):
            continue
        try:
            out.append(AIRentSuggestion(
                room_type=r.get("room_type"),
                rent_per_week=r.get("rent_per_week"),
                rent_per_month=r.get("rent_per_month"),
                currency=r.get("currency") or "GBP",
                academic_year=r.get("academic_year"),
                contract_length_weeks=r.get("contract_length_weeks"),
                confidence=float(r.get("confidence", 0.0)),
                reasoning=r.get("reasoning"),
            ))
        except (TypeError, ValueError):
            continue
    return out


def _build_suggestions(scheme: ExistingScheme, ai_result: dict) -> list[AIFieldSuggestion]:
    """Convert raw AI response into structured suggestion objects."""
    suggestions = []
    raw_suggestions = ai_result.get("suggestions", [])

    # Map of AI field names to scheme attributes for current-value lookup
    field_current_map = {
        "owner_company_name": scheme.owner_company.name if scheme.owner_company else None,
        "operator_company_name": scheme.operator_company.name if scheme.operator_company else None,
        "asset_manager_company_name": scheme.asset_manager_company.name if scheme.asset_manager_company else None,
        "landlord_company_name": scheme.landlord_company.name if scheme.landlord_company else None,
        "num_units": str(scheme.num_units) if scheme.num_units else None,
        "scheme_type": scheme.scheme_type,
        "status": scheme.status,
        "address": scheme.address,
        "postcode": scheme.postcode,
    }

    for s in raw_suggestions:
        field_name = s.get("field", "")
        if field_name not in field_current_map:
            continue

        suggested = s.get("suggested_value")
        confidence = s.get("confidence", 0.0)
        reasoning = s.get("reasoning", "")

        # Skip suggestions where AI returned same value as current
        current = field_current_map.get(field_name)
        if current and suggested and str(current).lower().strip() == str(suggested).lower().strip():
            continue

        suggestions.append(AIFieldSuggestion(
            field=field_name,
            current_value=current,
            suggested_value=str(suggested) if suggested is not None else None,
            confidence=min(max(confidence, 0.0), 1.0),
            reasoning=reasoning,
        ))

    return suggestions


def _find_or_note_company(name: str, db: Session) -> Optional[int]:
    """Try to find a company by name. Returns company ID or None."""
    if not name:
        return None
    normalized = name.strip().lower()
    company = (
        db.query(Company)
        .filter(Company.normalized_name == normalized)
        .first()
    )
    if company:
        return company.id

    # Fuzzy: check if the normalized name is contained within existing names
    company = (
        db.query(Company)
        .filter(Company.normalized_name.contains(normalized))
        .first()
    )
    if company:
        return company.id

    # Try reverse containment
    company = (
        db.query(Company)
        .filter(Company.normalized_name.op("~")(f".*{normalized}.*"))
        .first()
    )
    return company.id if company else None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/{scheme_id}/ai-enrich", response_model=AIEnrichmentResponse)
def ai_enrich_scheme(
    scheme_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Query AI to suggest missing data for a scheme. Returns suggestions
    for user review -- nothing is saved until the user applies them."""
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

    try:
        ai_result = _call_claude(scheme, db=db)
    except anthropic.BadRequestError as e:
        # Surface common quota / config issues with a meaningful 4xx so the
        # frontend can show the actual reason instead of a generic 500.
        msg = str(e)
        if "credit balance is too low" in msg.lower():
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail=(
                    "Anthropic credit balance is too low. "
                    "Top up at https://console.anthropic.com/settings/billing "
                    "and try again."
                ),
            )
        logger.error("ai_enrichment_bad_request", scheme_id=scheme.id, error=msg[:300])
        raise HTTPException(status_code=400, detail=f"Anthropic API rejected the request: {msg[:200]}")
    except anthropic.AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Anthropic API key is missing or invalid. Check ANTHROPIC_API_KEY in backend env.",
        )
    except anthropic.RateLimitError:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Anthropic rate-limited the request. Wait a moment and try again.",
        )
    except anthropic.APIConnectionError as e:
        logger.error("ai_enrichment_connection_error", scheme_id=scheme.id, error=str(e)[:200])
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not reach the Anthropic API. Check network connectivity.",
        )

    suggestions = _build_suggestions(scheme, ai_result)
    rents = _build_rent_suggestions(ai_result)

    return AIEnrichmentResponse(
        scheme_id=scheme.id,
        scheme_name=scheme.name,
        suggestions=suggestions,
        rents=rents,
        model_used=AI_MODEL,
        raw_ai_notes=ai_result.get("notes"),
        web_search_used=True,
    )


@router.post("/{scheme_id}/ai-enrich/apply", response_model=dict)
def apply_ai_suggestions(
    scheme_id: int,
    body: ApplySuggestionsRequest,
    current_user: User = Depends(require_role("admin", "bd_manager")),
    db: Session = Depends(get_db),
):
    """Apply user-approved AI suggestions to a scheme. Creates change-log
    entries for audit."""
    scheme = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.asset_manager_company),
            joinedload(ExistingScheme.landlord_company),
        )
        .filter(ExistingScheme.id == scheme_id)
        .first()
    )
    if not scheme:
        raise HTTPException(status_code=404, detail="Scheme not found")

    from app.scrapers.field_protection import (
        set_field,
        FieldValidationError,
    )

    ALLOWED_FIELDS = {
        "owner_company_name", "operator_company_name",
        "asset_manager_company_name", "landlord_company_name",
        "num_units", "scheme_type", "status",
        "address", "postcode",
    }

    # Alias company-name suggestions to their _company_id columns
    FK_ALIAS = {
        "owner_company_name": "owner_company_id",
        "operator_company_name": "operator_company_id",
        "asset_manager_company_name": "asset_manager_company_id",
        "landlord_company_name": "landlord_company_id",
    }

    applied = []
    skipped = []
    changed_by = f"user:{current_user.email}"

    for item in body.suggestions:
        if item.field not in ALLOWED_FIELDS:
            skipped.append({"field": item.field, "reason": "Field not allowed"})
            continue

        target_field = FK_ALIAS.get(item.field, item.field)
        target_value: Any = item.value

        # For company fields, resolve/create the Company and use its id
        if item.field in FK_ALIAS:
            if item.value:
                company_id = _find_or_note_company(item.value, db)
                if company_id is None:
                    new_company = Company(
                        name=str(item.value)[:255],
                        normalized_name=str(item.value).strip().lower()[:255],
                        company_type=_infer_company_type(item.field),
                    )
                    db.add(new_company)
                    db.flush()
                    company_id = new_company.id
                    logger.info(
                        "ai_enrichment_company_created",
                        company_name=item.value, company_id=company_id,
                    )
                target_value = company_id
            else:
                target_value = None

        try:
            did_apply = set_field(
                scheme, target_field, target_value,
                source="ai_enrichment", db=db,
                changed_by=changed_by,
            )
        except FieldValidationError as exc:
            skipped.append({"field": item.field, "reason": str(exc)})
            continue

        if did_apply:
            applied.append(item.field)
        else:
            skipped.append({"field": item.field, "reason": "Blocked by higher-precedence lock or no-op"})

    # Also persist any rent suggestions the user accepted (via body.rents)
    rents_saved = 0
    if getattr(body, "rents", None):
        from app.models.models import SchemeRent
        for r in body.rents:
            # Upsert on (scheme_id, room_type, academic_year)
            existing_rent = (
                db.query(SchemeRent)
                .filter(
                    SchemeRent.scheme_id == scheme_id,
                    SchemeRent.room_type == r.room_type,
                    SchemeRent.academic_year == r.academic_year,
                )
                .first()
            )
            if existing_rent:
                if r.rent_per_week is not None:
                    existing_rent.rent_per_week = r.rent_per_week
                if r.rent_per_month is not None:
                    existing_rent.rent_per_month = r.rent_per_month
                existing_rent.source = "ai_enrichment"
            else:
                db.add(SchemeRent(
                    scheme_id=scheme_id,
                    room_type=r.room_type,
                    rent_per_week=r.rent_per_week,
                    rent_per_month=r.rent_per_month,
                    currency=r.currency or "GBP",
                    academic_year=r.academic_year,
                    contract_length_weeks=r.contract_length_weeks,
                    source="ai_enrichment",
                ))
            rents_saved += 1

    db.commit()

    logger.info("ai_enrichment_applied",
                scheme_id=scheme_id, applied=applied, skipped=skipped, rents=rents_saved)

    return {
        "scheme_id": scheme_id,
        "applied_fields": applied,
        "skipped_fields": skipped,
        "rents_saved": rents_saved,
        "message": f"Applied {len(applied)} field suggestion(s) and {rents_saved} rent tier(s).",
    }


@router.post("/ai-enrich-batch", response_model=BatchEnrichResponse)
def ai_enrich_batch(
    body: BatchEnrichRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Enrich multiple schemes with AI in one request.
    Returns suggestions for all schemes (nothing is auto-saved)."""
    results: list[AIEnrichmentResponse] = []
    errors: list[dict] = []

    schemes = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.asset_manager_company),
            joinedload(ExistingScheme.landlord_company),
            joinedload(ExistingScheme.council),
        )
        .filter(ExistingScheme.id.in_(body.scheme_ids))
        .all()
    )

    found_ids = {s.id for s in schemes}
    for sid in body.scheme_ids:
        if sid not in found_ids:
            errors.append({"scheme_id": sid, "error": "Scheme not found"})

    for scheme in schemes:
        try:
            ai_result = _call_claude(scheme, db=db)
            suggestions = _build_suggestions(scheme, ai_result)
            rents = _build_rent_suggestions(ai_result)
            results.append(AIEnrichmentResponse(
                scheme_id=scheme.id,
                scheme_name=scheme.name,
                suggestions=suggestions,
                rents=rents,
                model_used=AI_MODEL,
                raw_ai_notes=ai_result.get("notes"),
                web_search_used=True,
            ))
        except HTTPException as exc:
            errors.append({"scheme_id": scheme.id, "error": exc.detail})
        except Exception as exc:
            logger.error("ai_enrichment_batch_error",
                         scheme_id=scheme.id, error=str(exc))
            errors.append({"scheme_id": scheme.id, "error": str(exc)})

    return BatchEnrichResponse(
        results=results,
        total_requested=len(body.scheme_ids),
        total_enriched=len(results),
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _infer_company_type(field_name: str) -> str:
    """Map an AI suggestion field to a company_type value."""
    mapping = {
        "owner_company_name": "Developer",
        "operator_company_name": "Operator",
        "asset_manager_company_name": "Investor",
        "landlord_company_name": "Investor",
    }
    return mapping.get(field_name, "Developer")
