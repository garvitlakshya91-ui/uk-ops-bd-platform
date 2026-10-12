"""The scheme registry: permanent IDs, aliases, source records, verification."""
from app.registry.core import (  # noqa: F401
    add_alias,
    alias_norm,
    find_by_alias,
    link_source_record,
    lock_field,
    mark_verified,
    match_record,
    propose_name,
    retire_duplicate,
)
