"""Scraper run model."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, Integer, String, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ScraperRun(Base):
    __tablename__ = "scraper_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    council_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("councils.id"), nullable=True
    )
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    records_found: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    records_created: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    records_updated: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    records_skipped: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    errors_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    error_details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
