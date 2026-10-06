"""Fuzzy matching of external property records to existing_schemes.

Shared by census reconciliation and rent attachment: postcode agreement
plus name-token overlap, greedy best-match.
"""
from __future__ import annotations

import re

STOP_TOKENS = {"the", "student", "students", "living", "accommodation",
               "halls", "hall", "house", "court"}

CITY_TOKENS = {"birmingham", "exeter", "leeds", "cardiff", "manchester",
               "london", "york", "durham", "lancaster", "chester"}


def norm_name(name: str) -> set[str]:
    tokens = re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split()
    return {t for t in tokens
            if t not in STOP_TOKENS and t not in CITY_TOKENS and len(t) > 1}


def norm_pc(pc: str | None) -> str:
    return re.sub(r"\s+", "", (pc or "").upper())


def name_score(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def build_index(schemes) -> list[dict]:
    """Index ORM schemes (or anything with .id/.name/.postcode)."""
    return [
        {"scheme": s, "tokens": norm_name(s.name), "pc": norm_pc(s.postcode)}
        for s in schemes
    ]


def best_match(index: list[dict], name: str, postcode: str | None,
               threshold: float = 0.5, used: set | None = None):
    """Return (scheme, score) or (None, 0). Postcode agreement adds 0.5."""
    tokens, pc = norm_name(name), norm_pc(postcode)
    best, best_score = None, 0.0
    for o in index:
        if used is not None and o["scheme"].id in used:
            continue
        score = name_score(tokens, o["tokens"])
        if pc and o["pc"] == pc:
            score += 0.5
        if score > best_score:
            best, best_score = o["scheme"], score
    if best is not None and best_score >= threshold:
        return best, round(best_score, 2)
    return None, 0.0
