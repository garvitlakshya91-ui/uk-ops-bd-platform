"""Alert model."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    alert_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    severity: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    planning_application_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("planning_applications.id"), nullable=True
    )
    company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=True
    )
    council_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("councils.id"), nullable=True
    )
    scheme_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("schemes.id"), nullable=True
    )
    is_read: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    is_actioned: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    actioned_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    actioned_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
