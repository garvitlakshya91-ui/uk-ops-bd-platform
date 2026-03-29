"""Council model."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Council(Base):
    __tablename__ = "councils"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    region: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    council_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    portal_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    portal_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    planning_policy_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    local_plan_status: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    article4_directions: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    btr_policy_exists: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    affordable_housing_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cil_charging: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    last_scraped_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    scrape_frequency_hours: Mapped[Optional[int]] = mapped_column(Integer, default=24)
    is_active: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
