import datetime
from typing import Optional, List

from sqlalchemy import (
    String,
    Integer,
    Float,
    Boolean,
    Text,
    DateTime,
    Date,
    ForeignKey,
    UniqueConstraint,
    Index,
    JSON,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Council(Base):
    __tablename__ = "councils"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    portal_type: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="idox, civica, nec, custom, api"
    )
    portal_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    scraper_class: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    region: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    last_scraped_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    scrape_frequency_hours: Mapped[int] = mapped_column(Integer, default=24, nullable=False)
    organisation_entity: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, unique=True,
        comment="Planning Data API organisation-entity code"
    )

    # Relationships
    applications: Mapped[List["PlanningApplication"]] = relationship(
        "PlanningApplication", back_populates="council", lazy="dynamic"
    )
    existing_schemes: Mapped[List["ExistingScheme"]] = relationship(
        "ExistingScheme", back_populates="council", lazy="dynamic"
    )
    scraper_runs: Mapped[List["ScraperRun"]] = relationship(
        "ScraperRun", back_populates="council", lazy="dynamic"
    )

    __table_args__ = (
        Index("ix_councils_region", "region"),
        Index("ix_councils_active", "active"),
    )


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    companies_house_number: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, unique=True
    )
    registered_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    website: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    sic_codes: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    company_type: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="Developer, Operator, Investor, RP, Agent, Consultant",
    )
    parent_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships
    parent_company: Mapped[Optional["Company"]] = relationship(
        "Company", remote_side="Company.id", backref="subsidiaries"
    )
    contacts: Mapped[List["Contact"]] = relationship(
        "Contact", back_populates="company", cascade="all, delete-orphan"
    )
    aliases: Mapped[List["CompanyAlias"]] = relationship(
        "CompanyAlias", back_populates="company", cascade="all, delete-orphan"
    )
    applicant_applications: Mapped[List["PlanningApplication"]] = relationship(
        "PlanningApplication",
        foreign_keys="PlanningApplication.applicant_company_id",
        back_populates="applicant_company",
        lazy="dynamic",
    )
    agent_applications: Mapped[List["PlanningApplication"]] = relationship(
        "PlanningApplication",
        foreign_keys="PlanningApplication.agent_company_id",
        back_populates="agent_company",
        lazy="dynamic",
    )
    operated_schemes: Mapped[List["ExistingScheme"]] = relationship(
        "ExistingScheme",
        foreign_keys="ExistingScheme.operator_company_id",
        back_populates="operator_company",
        lazy="dynamic",
    )
    owned_schemes: Mapped[List["ExistingScheme"]] = relationship(
        "ExistingScheme",
        foreign_keys="ExistingScheme.owner_company_id",
        back_populates="owner_company",
        lazy="dynamic",
    )
    pipeline_opportunities: Mapped[List["PipelineOpportunity"]] = relationship(
        "PipelineOpportunity", back_populates="company", lazy="dynamic"
    )

    __table_args__ = (
        Index("ix_companies_company_type", "company_type"),
        Index("ix_companies_name", "name"),
    )


class CompanyAlias(Base):
    __tablename__ = "company_aliases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    alias_name: Mapped[str] = mapped_column(String(500), nullable=False)
    source: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="scraper, manual, companies_house"
    )

    company: Mapped["Company"] = relationship("Company", back_populates="aliases")

    __table_args__ = (
        Index("ix_company_aliases_alias_name", "alias_name"),
    )


class Contact(Base):
    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    job_title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    linkedin_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    confidence_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    last_verified_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    company: Mapped["Company"] = relationship("Company", back_populates="contacts")

    __table_args__ = (
        Index("ix_contacts_company_id", "company_id"),
        Index("ix_contacts_email", "email"),
    )


class PlanningApplication(Base):
    __tablename__ = "planning_applications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reference: Mapped[str] = mapped_column(String(100), nullable=False)
    council_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("councils.id", ondelete="CASCADE"), nullable=False
    )
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    postcode: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    applicant_name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    applicant_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    agent_name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    agent_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    application_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    scheme_type: Mapped[str] = mapped_column(
        String(50),
        default="Unknown",
        nullable=False,
        comment="BTR, PBSA, Co-living, Senior, Affordable, Mixed, Unknown",
    )
    num_units: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    submission_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    decision_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    appeal_status: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    documents_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    raw_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships
    council: Mapped["Council"] = relationship("Council", back_populates="applications")
    applicant_company: Mapped[Optional["Company"]] = relationship(
        "Company",
        foreign_keys=[applicant_company_id],
        back_populates="applicant_applications",
    )
    agent_company: Mapped[Optional["Company"]] = relationship(
        "Company",
        foreign_keys=[agent_company_id],
        back_populates="agent_applications",
    )
    pipeline_opportunity: Mapped[Optional["PipelineOpportunity"]] = relationship(
        "PipelineOpportunity", back_populates="planning_application", uselist=False
    )

    __table_args__ = (
        UniqueConstraint("reference", "council_id", name="uq_application_reference_council"),
        Index("ix_planning_applications_postcode", "postcode"),
        Index("ix_planning_applications_scheme_type", "scheme_type"),
        Index("ix_planning_applications_status", "status"),
        Index("ix_planning_applications_submission_date", "submission_date"),
        Index("ix_planning_applications_council_id", "council_id"),
    )


class ExistingScheme(Base):
    __tablename__ = "existing_schemes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    postcode: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    council_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("councils.id", ondelete="SET NULL"), nullable=True
    )
    operator_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    owner_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    asset_manager_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    landlord_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    scheme_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, default="operational",
        comment="operational, under_construction, planned, decommissioned",
    )
    num_units: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    contract_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    contract_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    performance_rating: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    satisfaction_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    regulatory_rating: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    financial_health_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    epc_ratings: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="find_a_tender, contracts_finder, rsh, manual, epc",
    )
    source_reference: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True,
        comment="External ID: notice_id, contract reference, etc.",
    )
    last_verified_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    data_confidence_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="0.0-1.0 confidence in data accuracy"
    )
    # HM Land Registry CCOD fields — populated by the HMLR CCOD ingest job.
    hmlr_title_number: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, index=True,
        comment="HMLR title number matched from CCOD dataset",
    )
    hmlr_tenure: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True,
        comment="Freehold or Leasehold from HMLR CCOD",
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships
    council: Mapped[Optional["Council"]] = relationship(
        "Council", back_populates="existing_schemes"
    )
    operator_company: Mapped[Optional["Company"]] = relationship(
        "Company",
        foreign_keys=[operator_company_id],
        back_populates="operated_schemes",
    )
    owner_company: Mapped[Optional["Company"]] = relationship(
        "Company",
        foreign_keys=[owner_company_id],
        back_populates="owned_schemes",
    )
    asset_manager_company: Mapped[Optional["Company"]] = relationship(
        "Company",
        foreign_keys=[asset_manager_company_id],
        backref="asset_managed_schemes",
    )
    landlord_company: Mapped[Optional["Company"]] = relationship(
        "Company",
        foreign_keys=[landlord_company_id],
        backref="landlord_schemes",
    )
    pipeline_opportunity: Mapped[Optional["PipelineOpportunity"]] = relationship(
        "PipelineOpportunity", back_populates="scheme", uselist=False
    )
    contracts: Mapped[List["SchemeContract"]] = relationship(
        "SchemeContract", back_populates="scheme",
        cascade="all, delete-orphan",
    )
    change_logs: Mapped[List["SchemeChangeLog"]] = relationship(
        "SchemeChangeLog", back_populates="scheme",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_existing_schemes_postcode", "postcode"),
        Index("ix_existing_schemes_scheme_type", "scheme_type"),
        Index("ix_existing_schemes_contract_end_date", "contract_end_date"),
        Index("ix_existing_schemes_status", "status"),
        Index("ix_existing_schemes_source", "source"),
        Index("ix_existing_schemes_last_verified_at", "last_verified_at"),
        Index("ix_existing_schemes_asset_manager_company_id", "asset_manager_company_id"),
        Index("ix_existing_schemes_landlord_company_id", "landlord_company_id"),
    )


class SchemeContract(Base):
    """Tracks contract history — a scheme can have multiple sequential contracts."""
    __tablename__ = "scheme_contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scheme_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("existing_schemes.id", ondelete="CASCADE"), nullable=False
    )
    contract_reference: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    contract_type: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="management, maintenance, facilities, concession",
    )
    operator_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    client_company_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="SET NULL"), nullable=True
    )
    contract_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    contract_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    contract_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    currency: Mapped[str] = mapped_column(String(3), default="GBP", nullable=False)
    source: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="find_a_tender, contracts_finder, manual",
    )
    source_reference: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True, comment="notice_id or contract reference"
    )
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    raw_data: Mapped[Optional[dict]] = mapped_column(
        JSON, nullable=True, comment="Original scraped data for audit"
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships
    scheme: Mapped["ExistingScheme"] = relationship(
        "ExistingScheme", back_populates="contracts"
    )
    operator_company: Mapped[Optional["Company"]] = relationship(
        "Company", foreign_keys=[operator_company_id]
    )
    client_company: Mapped[Optional["Company"]] = relationship(
        "Company", foreign_keys=[client_company_id]
    )

    __table_args__ = (
        Index("ix_scheme_contracts_scheme_id", "scheme_id"),
        Index("ix_scheme_contracts_end_date", "contract_end_date"),
        Index("ix_scheme_contracts_source_reference", "source_reference"),
        Index("ix_scheme_contracts_is_current", "is_current"),
    )


class SchemeChangeLog(Base):
    """Audit trail for every field change on an ExistingScheme."""
    __tablename__ = "scheme_change_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scheme_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("existing_schemes.id", ondelete="CASCADE"), nullable=False
    )
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    old_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="find_a_tender, contracts_finder, rsh, epc, manual",
    )
    changed_by: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, comment="system, user email, task name"
    )
    changed_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    scheme: Mapped["ExistingScheme"] = relationship(
        "ExistingScheme", back_populates="change_logs"
    )

    __table_args__ = (
        Index("ix_scheme_change_logs_scheme_id", "scheme_id"),
        Index("ix_scheme_change_logs_changed_at", "changed_at"),
        Index("ix_scheme_change_logs_field_name", "field_name"),
    )


class PipelineOpportunity(Base):
    __tablename__ = "pipeline_opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="planning_application, existing_scheme"
    )
    planning_application_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("planning_applications.id", ondelete="SET NULL"), nullable=True
    )
    scheme_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("existing_schemes.id", ondelete="SET NULL"), nullable=True
    )
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    stage: Mapped[str] = mapped_column(
        String(50),
        default="identified",
        nullable=False,
        comment="identified, researched, contacted, meeting, proposal, won, lost",
    )
    priority: Mapped[str] = mapped_column(
        String(20), default="warm", nullable=False, comment="hot, warm, cold"
    )
    bd_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    assigned_to: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_contact_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    next_action: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    next_action_date: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships
    planning_application: Mapped[Optional["PlanningApplication"]] = relationship(
        "PlanningApplication", back_populates="pipeline_opportunity"
    )
    scheme: Mapped[Optional["ExistingScheme"]] = relationship(
        "ExistingScheme", back_populates="pipeline_opportunity"
    )
    company: Mapped["Company"] = relationship(
        "Company", back_populates="pipeline_opportunities"
    )

    __table_args__ = (
        Index("ix_pipeline_opportunities_stage", "stage"),
        Index("ix_pipeline_opportunities_priority", "priority"),
        Index("ix_pipeline_opportunities_company_id", "company_id"),
        Index("ix_pipeline_opportunities_next_action_date", "next_action_date"),
    )


class ScraperRun(Base):
    __tablename__ = "scraper_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    council_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("councils.id", ondelete="CASCADE"), nullable=True
    )
    started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    completed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(20),
        default="running",
        nullable=False,
        comment="running, success, failed, partial",
    )
    applications_found: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    applications_new: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    applications_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    errors_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_details: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Relationships
    council: Mapped["Council"] = relationship("Council", back_populates="scraper_runs")

    __table_args__ = (
        Index("ix_scraper_runs_council_id", "council_id"),
        Index("ix_scraper_runs_started_at", "started_at"),
        Index("ix_scraper_runs_status", "status"),
    )


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="new_application, status_change, contract_expiring, scraper_failure, new_opportunity",
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    entity_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    entity_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        Index("ix_alerts_type", "type"),
        Index("ix_alerts_is_read", "is_read"),
        Index("ix_alerts_created_at", "created_at"),
    )
