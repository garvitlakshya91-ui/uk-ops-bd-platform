"""Planning application model."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Boolean, Date, DateTime, Float, Integer, String, Text, ForeignKey, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class PlanningApplication(Base):
    __tablename__ = "planning_applications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    council_id: Mapped[int] = mapped_column(Integer, ForeignKey("councils.id"), nullable=False)
    reference: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    postcode: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    ward: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    applicant_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    applicant_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=True
    )
    agent_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    agent_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=True
    )
    application_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    scheme_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    decision: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    decision_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    submitted_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    validated_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    consultation_end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    committee_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    total_units: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    affordable_units: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    commercial_sqm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    storeys: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    epc_rating: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    portal_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    documents_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_btr: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    is_pbsa: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    is_affordable: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    bd_relevance_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
