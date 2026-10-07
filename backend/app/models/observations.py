"""Append-only scheme observations: the master database's write path.

Every fact about a scheme is recorded as a dated observation with its
source and basis (observed / derived / forecast). The per-field
provenance on existing_schemes is only a cache of the latest
publishable value, refreshed here.
"""
from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.census_sources import PUBLISHABLE_SOURCES
from app.models.models import ExistingScheme, SchemeObservation

BASES = ("observed", "derived", "forecast")


def record_observation(
    db: Session,
    scheme: ExistingScheme | int,
    field: str,
    value: Any,
    source: str,
    *,
    reference: Optional[str] = None,
    basis: str = "observed",
    academic_year: Optional[str] = None,
    refresh_cache: bool = True,
) -> SchemeObservation:
    """Append one observation and refresh the scheme's latest-value cache.

    `value` may be a number, a string or a JSON-able structure; it lands
    in the matching column. The cache (field_provenance) is updated only
    for publishable sources, so licensed benchmark figures never become
    the published value.
    """
    if basis not in BASES:
        raise ValueError(f"basis must be one of {BASES}")
    scheme_id = scheme if isinstance(scheme, int) else scheme.id
    row = SchemeObservation(
        scheme_id=scheme_id, field=field, source=source,
        source_reference=reference, basis=basis, academic_year=academic_year,
    )
    if isinstance(value, bool) or isinstance(value, (list, dict)):
        row.value_json = value
    elif isinstance(value, (int, float, Decimal)):
        row.value_num = value
        row.value_text = str(value)
    else:
        row.value_text = None if value is None else str(value)
    db.add(row)

    if refresh_cache and source in PUBLISHABLE_SOURCES:
        obj = scheme if isinstance(scheme, ExistingScheme) else db.get(ExistingScheme, scheme_id)
        if obj is not None:
            prov = dict(obj.field_provenance or {})
            prov[field] = {
                "value": value, "source": source, "basis": basis,
                "at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
                **({"ref": reference} if reference else {}),
            }
            obj.field_provenance = prov
            flag_modified(obj, "field_provenance")
    return row
