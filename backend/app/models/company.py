"""Company model."""

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    companies_house_number: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, unique=True
    )
    sector: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    sub_sector: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    company_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    website: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    headquarters_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    headquarters_postcode: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    employee_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    revenue_gbp: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    is_client: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    is_competitor: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    is_target: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    relationship_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    key_contact_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    key_contact_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    key_contact_phone: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    key_contact_title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
