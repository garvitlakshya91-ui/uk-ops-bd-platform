"""Pipeline opportunity model."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, Date, DateTime, Float, Integer, String, Text, ForeignKey, func,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class PipelineOpportunity(Base):
    __tablename__ = "pipeline_opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    planning_application_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("planning_applications.id"), nullable=True
    )
    company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=True
    )
    council_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("councils.id"), nullable=True
    )
    source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    stage: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    priority: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    estimated_units: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    estimated_value_gbp: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    expected_start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    expected_completion_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    probability_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    assigned_to: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_activity_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    next_action: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    next_action_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
