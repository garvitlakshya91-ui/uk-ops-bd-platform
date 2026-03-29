"""Initial migration - create all tables.

Revision ID: 001_initial
Revises: None
Create Date: 2026-03-22

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Councils table
    op.create_table(
        "councils",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(255), nullable=False, unique=True),
        sa.Column("region", sa.String(100), nullable=True),
        sa.Column("council_type", sa.String(50), nullable=True),
        sa.Column("portal_type", sa.String(100), nullable=True),
        sa.Column("portal_url", sa.Text(), nullable=True),
        sa.Column("planning_policy_url", sa.Text(), nullable=True),
        sa.Column("local_plan_status", sa.String(100), nullable=True),
        sa.Column("article4_directions", sa.Boolean(), default=False),
        sa.Column("btr_policy_exists", sa.Boolean(), default=False),
        sa.Column("affordable_housing_pct", sa.Float(), nullable=True),
        sa.Column("cil_charging", sa.Boolean(), default=False),
        sa.Column("last_scraped_at", sa.DateTime(), nullable=True),
        sa.Column("scrape_frequency_hours", sa.Integer(), default=24),
        sa.Column("is_active", sa.Boolean(), default=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    )
    op.create_index("ix_councils_region", "councils", ["region"])

    # Companies table
    op.create_table(
        "companies",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("companies_house_number", sa.String(20), nullable=True, unique=True),
        sa.Column("sector", sa.String(100), nullable=True),
        sa.Column("sub_sector", sa.String(100), nullable=True),
        sa.Column("company_type", sa.String(100), nullable=True),
        sa.Column("website", sa.Text(), nullable=True),
        sa.Column("headquarters_address", sa.Text(), nullable=True),
        sa.Column("headquarters_postcode", sa.String(10), nullable=True),
        sa.Column("phone", sa.String(30), nullable=True),
        sa.Column("employee_count", sa.Integer(), nullable=True),
        sa.Column("revenue_gbp", sa.BigInteger(), nullable=True),
        sa.Column("is_client", sa.Boolean(), default=False),
        sa.Column("is_competitor", sa.Boolean(), default=False),
        sa.Column("is_target", sa.Boolean(), default=False),
        sa.Column("relationship_status", sa.String(50), nullable=True),
        sa.Column("key_contact_name", sa.String(255), nullable=True),
        sa.Column("key_contact_email", sa.String(255), nullable=True),
        sa.Column("key_contact_phone", sa.String(30), nullable=True),
        sa.Column("key_contact_title", sa.String(255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    )
    op.create_index("ix_companies_sector", "companies", ["sector"])
    op.create_index("ix_companies_is_client", "companies", ["is_client"])

    # Planning applications table
    op.create_table(
        "planning_applications",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("council_id", sa.Integer(), sa.ForeignKey("councils.id"), nullable=False),
        sa.Column("reference", sa.String(100), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("postcode", sa.String(10), nullable=True),
        sa.Column("ward", sa.String(100), nullable=True),
        sa.Column("applicant_name", sa.String(255), nullable=True),
        sa.Column("applicant_company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=True),
        sa.Column("agent_name", sa.String(255), nullable=True),
        sa.Column("agent_company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=True),
        sa.Column("application_type", sa.String(100), nullable=True),
        sa.Column("scheme_type", sa.String(100), nullable=True),
        sa.Column("status", sa.String(100), nullable=True),
        sa.Column("decision", sa.String(100), nullable=True),
        sa.Column("decision_date", sa.Date(), nullable=True),
        sa.Column("submitted_date", sa.Date(), nullable=True),
        sa.Column("validated_date", sa.Date(), nullable=True),
        sa.Column("consultation_end_date", sa.Date(), nullable=True),
        sa.Column("committee_date", sa.Date(), nullable=True),
        sa.Column("total_units", sa.Integer(), nullable=True),
        sa.Column("affordable_units", sa.Integer(), nullable=True),
        sa.Column("commercial_sqm", sa.Float(), nullable=True),
        sa.Column("storeys", sa.Integer(), nullable=True),
        sa.Column("epc_rating", sa.String(5), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("portal_url", sa.Text(), nullable=True),
        sa.Column("documents_url", sa.Text(), nullable=True),
        sa.Column("is_btr", sa.Boolean(), default=False),
        sa.Column("is_pbsa", sa.Boolean(), default=False),
        sa.Column("is_affordable", sa.Boolean(), default=False),
        sa.Column("bd_relevance_score", sa.Float(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("raw_data", postgresql.JSONB(), nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    )
    op.create_index("ix_planning_applications_council_id", "planning_applications", ["council_id"])
    op.create_index("ix_planning_applications_reference", "planning_applications", ["reference"])
    op.create_index("ix_planning_applications_status", "planning_applications", ["status"])
    op.create_index("ix_planning_applications_scheme_type", "planning_applications", ["scheme_type"])
    op.create_index("ix_planning_applications_is_btr", "planning_applications", ["is_btr"])
    op.create_index("ix_planning_applications_is_pbsa", "planning_applications", ["is_pbsa"])
    op.create_index(
        "ix_planning_applications_submitted_date",
        "planning_applications",
        ["submitted_date"],
    )

    # Schemes (existing/operational)
    op.create_table(
        "schemes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("postcode", sa.String(10), nullable=True),
        sa.Column("council_id", sa.Integer(), sa.ForeignKey("councils.id"), nullable=True),
        sa.Column("owner_company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=True),
        sa.Column("operator_company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=True),
        sa.Column("scheme_type", sa.String(100), nullable=True),
        sa.Column("status", sa.String(100), nullable=True),
        sa.Column("total_units", sa.Integer(), nullable=True),
        sa.Column("unit_mix", postgresql.JSONB(), nullable=True),
        sa.Column("amenities", postgresql.JSONB(), nullable=True),
        sa.Column("completion_date", sa.Date(), nullable=True),
        sa.Column("contract_start_date", sa.Date(), nullable=True),
        sa.Column("contract_end_date", sa.Date(), nullable=True),
        sa.Column("contract_type", sa.String(100), nullable=True),
        sa.Column("annual_revenue_gbp", sa.BigInteger(), nullable=True),
        sa.Column("occupancy_pct", sa.Float(), nullable=True),
        sa.Column("avg_rent_pcm", sa.Float(), nullable=True),
        sa.Column("performance_rating", sa.Float(), nullable=True),
        sa.Column("nps_score", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    )
    op.create_index("ix_schemes_scheme_type", "schemes", ["scheme_type"])
    op.create_index("ix_schemes_status", "schemes", ["status"])
    op.create_index("ix_schemes_council_id", "schemes", ["council_id"])

    # Pipeline opportunities
    op.create_table(
        "pipeline_opportunities",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "planning_application_id",
            sa.Integer(),
            sa.ForeignKey("planning_applications.id"),
            nullable=True,
        ),
        sa.Column("company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=True),
        sa.Column("council_id", sa.Integer(), sa.ForeignKey("councils.id"), nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("stage", sa.String(100), nullable=True),
        sa.Column("priority", sa.String(50), nullable=True),
        sa.Column("estimated_units", sa.Integer(), nullable=True),
        sa.Column("estimated_value_gbp", sa.BigInteger(), nullable=True),
        sa.Column("expected_start_date", sa.Date(), nullable=True),
        sa.Column("expected_completion_date", sa.Date(), nullable=True),
        sa.Column("probability_pct", sa.Float(), nullable=True),
        sa.Column("assigned_to", sa.String(255), nullable=True),
        sa.Column("last_activity_date", sa.Date(), nullable=True),
        sa.Column("next_action", sa.Text(), nullable=True),
        sa.Column("next_action_date", sa.Date(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    )
    op.create_index("ix_pipeline_opportunities_stage", "pipeline_opportunities", ["stage"])
    op.create_index("ix_pipeline_opportunities_priority", "pipeline_opportunities", ["priority"])

    # Alerts
    op.create_table(
        "alerts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("alert_type", sa.String(100), nullable=True),
        sa.Column("severity", sa.String(50), nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column(
            "planning_application_id",
            sa.Integer(),
            sa.ForeignKey("planning_applications.id"),
            nullable=True,
        ),
        sa.Column("company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=True),
        sa.Column("council_id", sa.Integer(), sa.ForeignKey("councils.id"), nullable=True),
        sa.Column("scheme_id", sa.Integer(), sa.ForeignKey("schemes.id"), nullable=True),
        sa.Column("is_read", sa.Boolean(), default=False),
        sa.Column("is_actioned", sa.Boolean(), default=False),
        sa.Column("actioned_by", sa.String(255), nullable=True),
        sa.Column("actioned_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_alerts_alert_type", "alerts", ["alert_type"])
    op.create_index("ix_alerts_severity", "alerts", ["severity"])
    op.create_index("ix_alerts_is_read", "alerts", ["is_read"])

    # Scraper runs
    op.create_table(
        "scraper_runs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("council_id", sa.Integer(), sa.ForeignKey("councils.id"), nullable=True),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("records_found", sa.Integer(), default=0),
        sa.Column("records_created", sa.Integer(), default=0),
        sa.Column("records_updated", sa.Integer(), default=0),
        sa.Column("records_skipped", sa.Integer(), default=0),
        sa.Column("errors_count", sa.Integer(), default=0),
        sa.Column("error_details", postgresql.JSONB(), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_scraper_runs_source", "scraper_runs", ["source"])
    op.create_index("ix_scraper_runs_status", "scraper_runs", ["status"])
    op.create_index("ix_scraper_runs_council_id", "scraper_runs", ["council_id"])


def downgrade() -> None:
    op.drop_table("scraper_runs")
    op.drop_table("alerts")
    op.drop_table("pipeline_opportunities")
    op.drop_table("schemes")
    op.drop_table("planning_applications")
    op.drop_table("companies")
    op.drop_table("councils")
