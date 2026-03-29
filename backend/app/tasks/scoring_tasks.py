"""Celery tasks for BD scoring, alerting, and notifications.

Recalculates BD scores for all schemes and pipeline items, generates
alerts for actionable events, and dispatches notifications via email
and Slack.
"""

from __future__ import annotations

import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import httpx
import structlog
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.tasks import celery_app

logger = structlog.get_logger(__name__)


def _get_db() -> Session:
    """Create a new database session for use inside a Celery task."""
    return SessionLocal()


@celery_app.task(
    name="app.tasks.scoring_tasks.recalculate_all_scores",
    acks_late=True,
)
def recalculate_all_scores() -> dict[str, Any]:
    """Recalculate BD scores for all existing schemes and pipeline items.

    Updates the ``bd_score`` and ``priority`` fields on every
    :class:`PipelineOpportunity` record.

    Returns
    -------
    dict
        Summary with counts of items updated and any significant score
        changes detected.
    """
    db = _get_db()
    try:
        from app.models.models import ExistingScheme, PipelineOpportunity, PlanningApplication
        from app.scoring.bd_scorer import BDScorer

        scorer = BDScorer(db_session=db)

        # Recalculate for all pipeline opportunities.
        opportunities = db.query(PipelineOpportunity).all()
        updated = 0
        significant_changes = 0

        for opp in opportunities:
            old_score = opp.bd_score

            if opp.planning_application_id:
                app = db.query(PlanningApplication).get(opp.planning_application_id)
                if app:
                    opp.bd_score = scorer.score_planning_application(app)
            elif opp.scheme_id:
                scheme = db.query(ExistingScheme).get(opp.scheme_id)
                if scheme:
                    opp.bd_score = scorer.score_existing_scheme(scheme)

            if opp.bd_score is not None:
                updated += 1
                if old_score is not None and abs(opp.bd_score - old_score) >= 5.0:
                    significant_changes += 1

        # Re-prioritise.
        scored_opps = [o for o in opportunities if o.bd_score is not None]
        scored_opps.sort(key=lambda o: o.bd_score, reverse=True)

        total = len(scored_opps)
        if total > 0:
            hot_cutoff = max(1, int(total * 0.10))
            warm_cutoff = max(hot_cutoff + 1, int(total * 0.30))

            for i, opp in enumerate(scored_opps):
                if i < hot_cutoff:
                    opp.priority = "hot"
                elif i < warm_cutoff:
                    opp.priority = "warm"
                else:
                    opp.priority = "cold"

        db.commit()

        result = {
            "total_opportunities": len(opportunities),
            "updated": updated,
            "significant_changes": significant_changes,
        }
        logger.info("recalculate_all_scores_completed", **result)
        return result

    finally:
        db.close()


@celery_app.task(
    name="app.tasks.scoring_tasks.generate_alerts",
    acks_late=True,
)
def generate_alerts() -> dict[str, Any]:
    """Check for new alert conditions and create alert records.

    Conditions checked:

    * **Contract expiring** — schemes with contracts expiring within
      3 months that do not already have an active alert.
    * **New high-score application** — planning applications created in
      the last hour with a BD score above 70.
    * **Scraper failure** — scraping runs that failed in the last 2 hours.

    Returns
    -------
    dict
        Counts of alerts created by type.
    """
    db = _get_db()
    try:
        from app.models.models import Alert, ExistingScheme, PlanningApplication, ScraperRun
        from app.scoring.bd_scorer import BDScorer

        scorer = BDScorer(db_session=db)
        now = datetime.datetime.utcnow()
        today = datetime.date.today()
        alerts_created: dict[str, int] = {
            "contract_expiring": 0,
            "new_application": 0,
            "scraper_failure": 0,
        }

        # 1. Contracts expiring within 3 months.
        three_months = today + datetime.timedelta(days=90)
        expiring_schemes = (
            db.query(ExistingScheme)
            .filter(
                ExistingScheme.contract_end_date.isnot(None),
                ExistingScheme.contract_end_date <= three_months,
                ExistingScheme.contract_end_date >= today,
            )
            .all()
        )

        for scheme in expiring_schemes:
            # Check if an alert already exists for this scheme.
            existing_alert = (
                db.query(Alert)
                .filter(
                    Alert.type == "contract_expiring",
                    Alert.entity_type == "existing_scheme",
                    Alert.entity_id == scheme.id,
                    Alert.is_read.is_(False),
                )
                .first()
            )
            if existing_alert:
                continue

            days_left = (scheme.contract_end_date - today).days
            alert = Alert(
                type="contract_expiring",
                title=f"Contract expiring: {scheme.name} ({days_left} days remaining)",
                message=(
                    f"The management contract for {scheme.name} "
                    f"({scheme.num_units or '?'} units, {scheme.postcode or 'unknown location'}) "
                    f"expires on {scheme.contract_end_date}. "
                    f"Action required to prepare BD approach."
                ),
                entity_type="existing_scheme",
                entity_id=scheme.id,
                is_read=False,
            )
            db.add(alert)
            alerts_created["contract_expiring"] += 1

        # 2. New high-scoring planning applications (last hour).
        one_hour_ago = now - datetime.timedelta(hours=1)
        new_apps = (
            db.query(PlanningApplication)
            .filter(PlanningApplication.created_at >= one_hour_ago)
            .all()
        )

        for app in new_apps:
            score = scorer.score_planning_application(app)
            if score < 70:
                continue

            existing_alert = (
                db.query(Alert)
                .filter(
                    Alert.type == "new_application",
                    Alert.entity_type == "planning_application",
                    Alert.entity_id == app.id,
                )
                .first()
            )
            if existing_alert:
                continue

            alert = Alert(
                type="new_application",
                title=f"High-value application: {app.reference} (score: {score})",
                message=(
                    f"New planning application {app.reference} at "
                    f"{app.address or 'unknown address'} "
                    f"({app.scheme_type}, {app.num_units or '?'} units). "
                    f"BD score: {score}/100. Applicant: {app.applicant_name or 'Unknown'}."
                ),
                entity_type="planning_application",
                entity_id=app.id,
                is_read=False,
            )
            db.add(alert)
            alerts_created["new_application"] += 1

            # Send notification immediately for hot opportunities.
            if score >= 85:
                db.flush()
                send_alert_notifications.delay(alert.id)

        # 3. Scraper failures in the last 2 hours.
        two_hours_ago = now - datetime.timedelta(hours=2)
        failed_runs = (
            db.query(ScraperRun)
            .filter(
                ScraperRun.status == "failed",
                ScraperRun.started_at >= two_hours_ago,
            )
            .all()
        )

        for run in failed_runs:
            existing_alert = (
                db.query(Alert)
                .filter(
                    Alert.type == "scraper_failure",
                    Alert.entity_type == "scraper_run",
                    Alert.entity_id == run.id,
                )
                .first()
            )
            if existing_alert:
                continue

            council_name = run.council.name if run.council else "Unknown"
            alert = Alert(
                type="scraper_failure",
                title=f"Scraper failed: {council_name}",
                message=(
                    f"The scraper for {council_name} failed at "
                    f"{run.started_at}. "
                    f"Error details: {run.error_details or 'No details available'}."
                ),
                entity_type="scraper_run",
                entity_id=run.id,
                is_read=False,
            )
            db.add(alert)
            alerts_created["scraper_failure"] += 1

        db.commit()

        logger.info("generate_alerts_completed", **alerts_created)
        return alerts_created

    finally:
        db.close()


@celery_app.task(
    bind=True,
    name="app.tasks.scoring_tasks.send_alert_notifications",
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
)
def send_alert_notifications(self, alert_id: int) -> dict[str, Any]:
    """Send email and Slack notifications for an alert.

    Parameters
    ----------
    alert_id : int
        ID of the :class:`Alert` to send notifications for.

    Returns
    -------
    dict
        Status of email and Slack delivery.
    """
    db = _get_db()
    try:
        from app.models.models import Alert

        alert = db.query(Alert).get(alert_id)
        if not alert:
            logger.error("send_alert_not_found", alert_id=alert_id)
            return {"error": f"Alert {alert_id} not found"}

        log = logger.bind(alert_id=alert_id, alert_type=alert.type)
        result: dict[str, Any] = {"alert_id": alert_id}

        # 1. Send email notification.
        email_sent = _send_email_notification(alert)
        result["email"] = "sent" if email_sent else "failed"

        # 2. Send Slack notification.
        slack_sent = _send_slack_notification(alert)
        result["slack"] = "sent" if slack_sent else "failed"

        log.info("alert_notifications_sent", **result)
        return result

    except Exception as exc:
        logger.exception("send_alert_notifications_failed", alert_id=alert_id)
        raise self.retry(exc=exc)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Notification helpers
# ---------------------------------------------------------------------------

def _send_email_notification(alert) -> bool:
    """Send an email notification for the given alert.

    Returns ``True`` on success, ``False`` on failure.
    """
    if not settings.SMTP_USERNAME or not settings.SMTP_PASSWORD:
        logger.warning("email_notification_skipped_no_smtp_config")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[UK Ops BD] {alert.title}"
        msg["From"] = settings.SMTP_FROM_EMAIL
        msg["To"] = settings.SMTP_FROM_EMAIL  # Default recipient; in production, use a distribution list.

        # Plain text body.
        text_body = f"{alert.title}\n\n{alert.message or ''}\n\nType: {alert.type}\nCreated: {alert.created_at}"

        # HTML body.
        html_body = f"""
        <html>
        <body>
            <h2 style="color: #1a5276;">{alert.title}</h2>
            <p>{alert.message or ''}</p>
            <hr>
            <p><small>Alert type: {alert.type} | Created: {alert.created_at}</small></p>
            <p><small>UK Ops BD Platform</small></p>
        </body>
        </html>
        """

        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
            if settings.SMTP_USE_TLS:
                server.starttls()
            server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
            server.send_message(msg)

        logger.info("email_notification_sent", alert_id=alert.id)
        return True

    except Exception:
        logger.exception("email_notification_failed", alert_id=alert.id)
        return False


def _send_slack_notification(alert) -> bool:
    """Send a Slack webhook notification for the given alert.

    Returns ``True`` on success, ``False`` on failure.
    """
    if not settings.SLACK_WEBHOOK_URL:
        logger.warning("slack_notification_skipped_no_webhook")
        return False

    # Map alert types to emoji.
    emoji_map = {
        "contract_expiring": ":warning:",
        "new_application": ":tada:",
        "scraper_failure": ":x:",
        "new_opportunity": ":star:",
    }
    emoji = emoji_map.get(alert.type, ":bell:")

    payload = {
        "text": f"{emoji} *{alert.title}*",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": alert.title,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": alert.message or "_No details_",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Type: `{alert.type}` | Created: {alert.created_at}",
                    }
                ],
            },
        ],
    }

    try:
        import httpx

        with httpx.Client(timeout=10.0) as client:
            resp = client.post(settings.SLACK_WEBHOOK_URL, json=payload)
            resp.raise_for_status()

        logger.info("slack_notification_sent", alert_id=alert.id)
        return True

    except Exception:
        logger.exception("slack_notification_failed", alert_id=alert.id)
        return False
