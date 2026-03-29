"""Alert generation, notification dispatch, and message formatting for the
UK Ops BD Platform monitoring subsystem."""

from __future__ import annotations

import datetime
import json
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import httpx
import structlog
from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from app.config import settings
from app.models.models import (
    Alert,
    Council,
    ExistingScheme,
    PipelineOpportunity,
    PlanningApplication,
    ScraperRun,
)
from app.monitoring.health_checker import ScraperHealthMonitor

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Email templates
# ---------------------------------------------------------------------------

_DIGEST_EMAIL_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<style>
  body {{ font-family: Arial, Helvetica, sans-serif; color: #333; line-height: 1.5; }}
  .container {{ max-width: 640px; margin: 0 auto; padding: 20px; }}
  .header {{ background: #1a365d; color: #fff; padding: 16px 24px; border-radius: 6px 6px 0 0; }}
  .header h1 {{ margin: 0; font-size: 20px; }}
  .body {{ background: #f7fafc; padding: 24px; border: 1px solid #e2e8f0; }}
  .alert-card {{ background: #fff; border-left: 4px solid {border_color}; padding: 12px 16px;
                 margin-bottom: 12px; border-radius: 0 4px 4px 0; }}
  .alert-card.critical {{ border-left-color: #e53e3e; }}
  .alert-card.warning {{ border-left-color: #dd6b20; }}
  .alert-card.info {{ border-left-color: #3182ce; }}
  .alert-title {{ font-weight: 600; font-size: 14px; margin-bottom: 4px; }}
  .alert-message {{ font-size: 13px; color: #4a5568; }}
  .alert-time {{ font-size: 11px; color: #a0aec0; }}
  .footer {{ font-size: 12px; color: #a0aec0; padding: 16px 24px; text-align: center; }}
  .stat {{ display: inline-block; text-align: center; margin: 0 16px; }}
  .stat-value {{ font-size: 28px; font-weight: 700; color: #1a365d; }}
  .stat-label {{ font-size: 12px; color: #718096; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>UK Ops BD Platform &mdash; Alert Digest</h1>
    <p style="margin:4px 0 0;font-size:13px;color:#cbd5e0;">{date}</p>
  </div>
  <div class="body">
    <div style="text-align:center;padding:12px 0 20px;">
      <div class="stat"><div class="stat-value">{total_alerts}</div><div class="stat-label">Alerts</div></div>
      <div class="stat"><div class="stat-value">{critical_count}</div><div class="stat-label">Critical</div></div>
      <div class="stat"><div class="stat-value">{warning_count}</div><div class="stat-label">Warning</div></div>
    </div>
    {alert_cards}
  </div>
  <div class="footer">
    This is an automated message from UK Ops BD Platform.
    <br>Manage notification preferences in your dashboard settings.
  </div>
</div>
</body>
</html>
"""

_SINGLE_ALERT_EMAIL_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<style>
  body {{ font-family: Arial, Helvetica, sans-serif; color: #333; line-height: 1.5; }}
  .container {{ max-width: 560px; margin: 0 auto; padding: 20px; }}
  .header {{ background: #1a365d; color: #fff; padding: 14px 20px; border-radius: 6px 6px 0 0; }}
  .body {{ padding: 20px; border: 1px solid #e2e8f0; border-top: none; }}
  .footer {{ font-size: 11px; color: #a0aec0; padding: 12px 20px; text-align: center; }}
</style>
</head>
<body>
<div class="container">
  <div class="header"><strong>{title}</strong></div>
  <div class="body">
    <p>{message}</p>
    <p style="font-size:12px;color:#718096;">Type: {alert_type} | {timestamp}</p>
  </div>
  <div class="footer">UK Ops BD Platform &mdash; automated alert</div>
</div>
</body>
</html>
"""

_ALERT_CARD_TEMPLATE = """\
<div class="alert-card {severity}">
  <div class="alert-title">{title}</div>
  <div class="alert-message">{message}</div>
  <div class="alert-time">{timestamp}</div>
</div>
"""


def _severity_for_type(alert_type: str) -> str:
    critical_types = {"scraper_failure"}
    warning_types = {"contract_expiring", "status_change"}
    if alert_type in critical_types:
        return "critical"
    if alert_type in warning_types:
        return "warning"
    return "info"


def _border_color_for_severity(severity: str) -> str:
    return {
        "critical": "#e53e3e",
        "warning": "#dd6b20",
        "info": "#3182ce",
    }.get(severity, "#3182ce")


# ---------------------------------------------------------------------------
# NotificationService
# ---------------------------------------------------------------------------


class NotificationService:
    """Delivers formatted alert messages via email (SMTP) and Slack."""

    def send_email(self, to: str, subject: str, body: str) -> bool:
        """Send an HTML email via configured SMTP server."""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = settings.SMTP_FROM_EMAIL
            msg["To"] = to
            msg.attach(MIMEText(body, "html"))

            context = ssl.create_default_context()
            if settings.SMTP_USE_TLS:
                with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
                    server.ehlo()
                    server.starttls(context=context)
                    server.ehlo()
                    if settings.SMTP_USERNAME:
                        server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
                    server.sendmail(settings.SMTP_FROM_EMAIL, to, msg.as_string())
            else:
                with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
                    if settings.SMTP_USERNAME:
                        server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
                    server.sendmail(settings.SMTP_FROM_EMAIL, to, msg.as_string())

            log.info("email_sent", to=to, subject=subject)
            return True
        except Exception:
            log.exception("email_send_failed", to=to, subject=subject)
            return False

    def send_slack(self, webhook_url: str, message: dict[str, Any]) -> bool:
        """Post a Block Kit message payload to a Slack incoming webhook."""
        try:
            resp = httpx.post(webhook_url, json=message, timeout=10)
            resp.raise_for_status()
            log.info("slack_message_sent", webhook=webhook_url[:40])
            return True
        except Exception:
            log.exception("slack_send_failed", webhook=webhook_url[:40])
            return False

    # -- Formatters --------------------------------------------------------

    def format_alert_email(self, alert: Alert) -> str:
        """Render a single-alert HTML email body."""
        return _SINGLE_ALERT_EMAIL_TEMPLATE.format(
            title=alert.title,
            message=alert.message or "",
            alert_type=alert.type,
            timestamp=alert.created_at.strftime("%Y-%m-%d %H:%M UTC")
            if alert.created_at
            else "",
        )

    def format_digest_email(self, alerts: list[Alert]) -> str:
        """Render a multi-alert digest HTML email body."""
        cards_html = ""
        critical = warning = 0
        for a in alerts:
            sev = _severity_for_type(a.type)
            if sev == "critical":
                critical += 1
            elif sev == "warning":
                warning += 1
            cards_html += _ALERT_CARD_TEMPLATE.format(
                severity=sev,
                title=a.title,
                message=a.message or "",
                timestamp=a.created_at.strftime("%Y-%m-%d %H:%M UTC")
                if a.created_at
                else "",
            )

        return _DIGEST_EMAIL_TEMPLATE.format(
            date=datetime.datetime.now(datetime.timezone.utc).strftime(
                "%A, %d %B %Y"
            ),
            total_alerts=len(alerts),
            critical_count=critical,
            warning_count=warning,
            alert_cards=cards_html,
            border_color="#1a365d",
        )

    def format_alert_slack(self, alert: Alert) -> dict[str, Any]:
        """Build a Slack Block Kit message for a single alert."""
        severity = _severity_for_type(alert.type)
        emoji = {"critical": ":red_circle:", "warning": ":large_orange_circle:", "info": ":large_blue_circle:"}.get(
            severity, ":large_blue_circle:"
        )
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji}  {alert.title}",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Type:*\n{alert.type}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Severity:*\n{severity.title()}",
                        },
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": alert.message or "_No additional details._",
                    },
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"UK Ops BD Platform | {alert.created_at.strftime('%Y-%m-%d %H:%M UTC') if alert.created_at else 'now'}",
                        }
                    ],
                },
            ]
        }


# ---------------------------------------------------------------------------
# AlertManager
# ---------------------------------------------------------------------------


class AlertManager:
    """Generates alerts by inspecting live data and persists them."""

    def __init__(self, db: Session) -> None:
        self.db = db
        self.health_monitor = ScraperHealthMonitor(db)
        self.notifications = NotificationService()

    def _create_alert(
        self,
        alert_type: str,
        title: str,
        message: str,
        entity_type: str | None = None,
        entity_id: int | None = None,
    ) -> Alert:
        alert = Alert(
            type=alert_type,
            title=title,
            message=message,
            entity_type=entity_type,
            entity_id=entity_id,
            is_read=False,
        )
        self.db.add(alert)
        self.db.commit()
        self.db.refresh(alert)
        log.info("alert_created", type=alert_type, title=title, alert_id=alert.id)
        return alert

    # -- Check routines ----------------------------------------------------

    def check_scraper_health(self) -> list[Alert]:
        """Generate alerts for scrapers that are failing or anomalous."""
        alerts: list[Alert] = []
        failing = self.health_monitor.get_failing_scrapers()

        for h in failing:
            council_name = h.get("council_name", f"Council #{h['council_id']}")
            reasons: list[str] = []
            if h["success_rate"] < 70:
                reasons.append(f"success rate {h['success_rate']}%")
            if h["consecutive_failures"] > 3:
                reasons.append(
                    f"{h['consecutive_failures']} consecutive failures"
                )

            alert = self._create_alert(
                alert_type="scraper_failure",
                title=f"Scraper unhealthy: {council_name}",
                message=f"Council scraper for {council_name} is failing. "
                + "; ".join(reasons) + ".",
                entity_type="council",
                entity_id=h["council_id"],
            )
            alerts.append(alert)

        # Anomaly-based alerts for all active councils
        councils = (
            self.db.query(Council).filter(Council.active.is_(True)).all()
        )
        for council in councils:
            anomalies = self.health_monitor.detect_anomalies(council.id)
            for anomaly_text in anomalies:
                alert = self._create_alert(
                    alert_type="scraper_failure",
                    title=f"Scraper anomaly: {council.name}",
                    message=anomaly_text,
                    entity_type="council",
                    entity_id=council.id,
                )
                alerts.append(alert)

        log.info("scraper_health_checked", alerts_created=len(alerts))
        return alerts

    def check_contract_expiries(self) -> list[Alert]:
        """Generate alerts for schemes whose contracts expire within 3, 6, or 12 months."""
        alerts: list[Alert] = []
        today = datetime.date.today()
        thresholds = [
            (3, "3 months"),
            (6, "6 months"),
            (12, "12 months"),
        ]

        for months, label in thresholds:
            cutoff = today + datetime.timedelta(days=months * 30)
            # Only look at contracts we have not yet alerted for in this window.
            # Simple approach: alert if expiry falls within the window and no
            # matching unread alert already exists.
            schemes = (
                self.db.query(ExistingScheme)
                .filter(
                    ExistingScheme.contract_end_date.isnot(None),
                    ExistingScheme.contract_end_date <= cutoff,
                    ExistingScheme.contract_end_date >= today,
                )
                .all()
            )

            for scheme in schemes:
                # Avoid duplicate alerts for the same scheme/threshold.
                existing = (
                    self.db.query(Alert)
                    .filter(
                        Alert.type == "contract_expiring",
                        Alert.entity_type == "scheme",
                        Alert.entity_id == scheme.id,
                        Alert.is_read.is_(False),
                    )
                    .first()
                )
                if existing:
                    continue

                days_left = (scheme.contract_end_date - today).days
                alert = self._create_alert(
                    alert_type="contract_expiring",
                    title=f"Contract expiring: {scheme.name}",
                    message=(
                        f"Scheme \"{scheme.name}\" contract expires on "
                        f"{scheme.contract_end_date.isoformat()} "
                        f"({days_left} days remaining, within {label} window)."
                    ),
                    entity_type="scheme",
                    entity_id=scheme.id,
                )
                alerts.append(alert)

        log.info("contract_expiry_checked", alerts_created=len(alerts))
        return alerts

    def check_new_applications(
        self,
        min_units: int = 50,
        scheme_types: list[str] | None = None,
        lookback_hours: int = 24,
    ) -> list[Alert]:
        """Alert for new planning applications matching BD criteria."""
        if scheme_types is None:
            scheme_types = ["BTR", "PBSA", "Co-living", "Senior"]

        alerts: list[Alert] = []
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=lookback_hours
        )

        query = self.db.query(PlanningApplication).filter(
            PlanningApplication.created_at >= cutoff,
        )

        if scheme_types:
            query = query.filter(
                PlanningApplication.scheme_type.in_(scheme_types)
            )

        if min_units:
            query = query.filter(
                PlanningApplication.num_units >= min_units,
            )

        applications = query.all()

        for app in applications:
            # Skip if alert already exists for this application
            existing = (
                self.db.query(Alert)
                .filter(
                    Alert.type == "new_application",
                    Alert.entity_type == "planning_application",
                    Alert.entity_id == app.id,
                )
                .first()
            )
            if existing:
                continue

            council = self.db.get(Council, app.council_id)
            council_name = council.name if council else "Unknown"

            alert = self._create_alert(
                alert_type="new_application",
                title=f"New {app.scheme_type} application: {app.num_units or '?'} units in {council_name}",
                message=(
                    f"Reference: {app.reference}\n"
                    f"Address: {app.address or 'N/A'}\n"
                    f"Description: {(app.description or '')[:200]}\n"
                    f"Applicant: {app.applicant_name or 'N/A'}\n"
                    f"Units: {app.num_units or 'N/A'} | Type: {app.scheme_type}"
                ),
                entity_type="planning_application",
                entity_id=app.id,
            )
            alerts.append(alert)

        log.info("new_applications_checked", alerts_created=len(alerts))
        return alerts

    def check_status_changes(self, lookback_hours: int = 24) -> list[Alert]:
        """Alert when application status changes to a decision (approved/refused)."""
        alerts: list[Alert] = []
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=lookback_hours
        )

        decision_statuses = [
            "Approved",
            "Refused",
            "Granted",
            "Permitted",
            "Withdrawn",
            "Appeal Allowed",
            "Appeal Dismissed",
        ]

        applications = (
            self.db.query(PlanningApplication)
            .filter(
                PlanningApplication.updated_at >= cutoff,
                PlanningApplication.status.in_(decision_statuses),
            )
            .all()
        )

        for app in applications:
            existing = (
                self.db.query(Alert)
                .filter(
                    Alert.type == "status_change",
                    Alert.entity_type == "planning_application",
                    Alert.entity_id == app.id,
                    Alert.created_at >= cutoff,
                )
                .first()
            )
            if existing:
                continue

            council = self.db.get(Council, app.council_id)
            council_name = council.name if council else "Unknown"

            alert = self._create_alert(
                alert_type="status_change",
                title=f"Application {app.status}: {app.reference}",
                message=(
                    f"Application {app.reference} in {council_name} has been "
                    f"{app.status}.\n"
                    f"Address: {app.address or 'N/A'}\n"
                    f"Type: {app.scheme_type} | Units: {app.num_units or 'N/A'}\n"
                    f"Decision date: {app.decision_date or 'N/A'}"
                ),
                entity_type="planning_application",
                entity_id=app.id,
            )
            alerts.append(alert)

        log.info("status_changes_checked", alerts_created=len(alerts))
        return alerts

    def check_data_quality(self, threshold: float = 60.0) -> list[Alert]:
        """Alert when a council's data quality score drops below threshold."""
        alerts: list[Alert] = []
        all_health = self.health_monitor.get_all_health()

        for h in all_health:
            if h["data_quality_score"] < threshold and h["total_runs_7d"] > 0:
                council_name = h.get(
                    "council_name", f"Council #{h['council_id']}"
                )
                # Deduplicate: only alert once per council per day
                today_start = datetime.datetime.combine(
                    datetime.date.today(),
                    datetime.time.min,
                    tzinfo=datetime.timezone.utc,
                )
                existing = (
                    self.db.query(Alert)
                    .filter(
                        Alert.type == "scraper_failure",
                        Alert.entity_type == "council",
                        Alert.entity_id == h["council_id"],
                        Alert.title.contains("Data quality"),
                        Alert.created_at >= today_start,
                    )
                    .first()
                )
                if existing:
                    continue

                alert = self._create_alert(
                    alert_type="scraper_failure",
                    title=f"Data quality low: {council_name}",
                    message=(
                        f"Data quality score for {council_name} is "
                        f"{h['data_quality_score']}% (threshold: {threshold}%). "
                        f"Key fields are missing on recently scraped applications."
                    ),
                    entity_type="council",
                    entity_id=h["council_id"],
                )
                alerts.append(alert)

        log.info("data_quality_checked", alerts_created=len(alerts))
        return alerts

    # -- Convenience -------------------------------------------------------

    def run_all_checks(
        self,
        notify_email: str | None = None,
        notify_slack: bool = False,
    ) -> list[Alert]:
        """Execute every check routine and optionally dispatch notifications."""
        all_alerts: list[Alert] = []
        all_alerts.extend(self.check_scraper_health())
        all_alerts.extend(self.check_contract_expiries())
        all_alerts.extend(self.check_new_applications())
        all_alerts.extend(self.check_status_changes())
        all_alerts.extend(self.check_data_quality())

        if not all_alerts:
            log.info("all_checks_completed", alerts=0)
            return all_alerts

        # Email digest
        if notify_email:
            body = self.notifications.format_digest_email(all_alerts)
            self.notifications.send_email(
                to=notify_email,
                subject=f"UK Ops BD Alert Digest - {len(all_alerts)} alerts",
                body=body,
            )

        # Slack (individual messages for critical, digest summary for others)
        if notify_slack and settings.SLACK_WEBHOOK_URL:
            for alert in all_alerts:
                payload = self.notifications.format_alert_slack(alert)
                self.notifications.send_slack(
                    settings.SLACK_WEBHOOK_URL, payload
                )

        log.info("all_checks_completed", alerts=len(all_alerts))
        return all_alerts
