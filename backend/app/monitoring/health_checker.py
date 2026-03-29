"""Scraper health monitoring – tracks per-council scraper reliability,
detects anomalies, and generates system-wide health reports."""

from __future__ import annotations

import datetime
from typing import Any

import structlog
from sqlalchemy import func, and_, case, select
from sqlalchemy.orm import Session

from app.models.models import (
    Council,
    PlanningApplication,
    ScraperRun,
)

log = structlog.get_logger(__name__)

# Key fields used to evaluate data quality on scraped applications.
_APPLICATION_KEY_FIELDS: list[str] = [
    "address",
    "postcode",
    "description",
    "applicant_name",
    "application_type",
    "status",
    "scheme_type",
    "num_units",
    "submission_date",
]

# Thresholds
_ROLLING_WINDOW_DAYS = 7
_FAILURE_SUCCESS_RATE_THRESHOLD = 0.70
_FAILURE_CONSECUTIVE_THRESHOLD = 3
_ANOMALY_APP_DROP_FACTOR = 0.50  # >50 % decrease from rolling avg
_ANOMALY_RESPONSE_TIME_FACTOR = 2.0  # 2x the rolling avg


class ScraperHealthMonitor:
    """Aggregates scraper-run telemetry and exposes health metrics per council."""

    def __init__(self, db: Session) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Record
    # ------------------------------------------------------------------

    def record_run(self, scraper_run: ScraperRun) -> None:
        """Persist a completed scraper run and update the council timestamp."""
        self.db.add(scraper_run)

        if scraper_run.status == "success":
            council = self.db.get(Council, scraper_run.council_id)
            if council is not None:
                council.last_scraped_at = scraper_run.completed_at or datetime.datetime.now(
                    datetime.timezone.utc
                )

        self.db.commit()
        log.info(
            "scraper_run_recorded",
            council_id=scraper_run.council_id,
            status=scraper_run.status,
            applications_found=scraper_run.applications_found,
        )

    # ------------------------------------------------------------------
    # Health status helpers
    # ------------------------------------------------------------------

    def _rolling_window_start(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=_ROLLING_WINDOW_DAYS
        )

    def _council_runs(self, council_id: int) -> list[ScraperRun]:
        """Return runs for a council in the rolling window, newest first."""
        cutoff = self._rolling_window_start()
        return (
            self.db.query(ScraperRun)
            .filter(
                ScraperRun.council_id == council_id,
                ScraperRun.started_at >= cutoff,
            )
            .order_by(ScraperRun.started_at.desc())
            .all()
        )

    @staticmethod
    def _success_rate(runs: list[ScraperRun]) -> float:
        if not runs:
            return 0.0
        successes = sum(1 for r in runs if r.status == "success")
        return round(successes / len(runs) * 100, 2)

    @staticmethod
    def _avg_applications_found(runs: list[ScraperRun]) -> float:
        successful = [r for r in runs if r.status == "success"]
        if not successful:
            return 0.0
        return round(sum(r.applications_found for r in successful) / len(successful), 2)

    @staticmethod
    def _last_successful_run(runs: list[ScraperRun]) -> datetime.datetime | None:
        for r in runs:
            if r.status == "success" and r.completed_at is not None:
                return r.completed_at
        return None

    @staticmethod
    def _consecutive_failures(runs: list[ScraperRun]) -> int:
        count = 0
        for r in runs:
            if r.status in ("failed",):
                count += 1
            else:
                break
        return count

    @staticmethod
    def _response_time_avg(runs: list[ScraperRun]) -> float | None:
        durations = [
            r.duration_seconds
            for r in runs
            if r.duration_seconds is not None and r.status == "success"
        ]
        if not durations:
            return None
        return round(sum(durations) / len(durations), 2)

    def _data_quality_score(self, council_id: int) -> float:
        """Percentage of applications (last 7 days) with all key fields populated."""
        cutoff = self._rolling_window_start()
        apps = (
            self.db.query(PlanningApplication)
            .filter(
                PlanningApplication.council_id == council_id,
                PlanningApplication.created_at >= cutoff,
            )
            .all()
        )
        if not apps:
            return 100.0  # no data to judge

        total_fields = len(_APPLICATION_KEY_FIELDS) * len(apps)
        populated = 0
        for app in apps:
            for field in _APPLICATION_KEY_FIELDS:
                value = getattr(app, field, None)
                if value is not None and value != "" and value != "Unknown":
                    populated += 1

        return round(populated / total_fields * 100, 2)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_health_status(self, council_id: int) -> dict[str, Any]:
        """Current health metrics for a single council."""
        runs = self._council_runs(council_id)
        council = self.db.get(Council, council_id)
        return {
            "council_id": council_id,
            "council_name": council.name if council else None,
            "success_rate": self._success_rate(runs),
            "avg_applications_found": self._avg_applications_found(runs),
            "last_successful_run": self._last_successful_run(runs),
            "consecutive_failures": self._consecutive_failures(runs),
            "response_time_avg": self._response_time_avg(runs),
            "data_quality_score": self._data_quality_score(council_id),
            "total_runs_7d": len(runs),
        }

    def get_all_health(self) -> list[dict[str, Any]]:
        """Health summary for every active council."""
        councils = (
            self.db.query(Council).filter(Council.active.is_(True)).all()
        )
        results = []
        for council in councils:
            results.append(self.get_health_status(council.id))
        return results

    def get_failing_scrapers(self) -> list[dict[str, Any]]:
        """Councils where success_rate < 70 % or consecutive_failures > 3."""
        all_health = self.get_all_health()
        failing = []
        for h in all_health:
            if (
                h["success_rate"] < _FAILURE_SUCCESS_RATE_THRESHOLD * 100
                or h["consecutive_failures"] > _FAILURE_CONSECUTIVE_THRESHOLD
            ):
                failing.append(h)
        return failing

    def detect_anomalies(self, council_id: int) -> list[str]:
        """Detect operational anomalies for a given council scraper."""
        anomalies: list[str] = []
        runs = self._council_runs(council_id)

        if len(runs) < 2:
            return anomalies

        latest = runs[0]
        historical = runs[1:]

        # 1. Sudden drop in applications found
        avg_apps = self._avg_applications_found(historical)
        if (
            avg_apps > 0
            and latest.status == "success"
            and latest.applications_found < avg_apps * _ANOMALY_APP_DROP_FACTOR
        ):
            anomalies.append(
                f"Application count dropped to {latest.applications_found} "
                f"from rolling average of {avg_apps:.0f} "
                f"({(1 - latest.applications_found / avg_apps) * 100:.0f}% decrease)"
            )

        # 2. Error spike suggesting HTML structure change
        historical_errors = [r.errors_count for r in historical if r.errors_count is not None]
        avg_errors = (
            sum(historical_errors) / len(historical_errors) if historical_errors else 0
        )
        if latest.errors_count > 0 and (
            avg_errors == 0 or latest.errors_count > avg_errors * 3
        ):
            error_types = ""
            if latest.error_details and isinstance(latest.error_details, dict):
                error_types = ", ".join(
                    f"{k}: {v}" for k, v in latest.error_details.items()
                )
            anomalies.append(
                f"Error count spiked to {latest.errors_count} "
                f"(avg {avg_errors:.1f}). Possible HTML structure change. "
                f"{error_types}".strip()
            )

        # 3. Response time increase suggesting blocking
        avg_rt = self._response_time_avg(historical)
        if (
            avg_rt is not None
            and latest.duration_seconds is not None
            and latest.duration_seconds > avg_rt * _ANOMALY_RESPONSE_TIME_FACTOR
        ):
            anomalies.append(
                f"Response time {latest.duration_seconds:.1f}s is "
                f"{latest.duration_seconds / avg_rt:.1f}x the average "
                f"({avg_rt:.1f}s). Possible rate-limiting or blocking."
            )

        # 4. Missing fields that were previously populated
        quality_now = self._data_quality_score(council_id)
        # Compare with a longer lookback by checking older apps
        older_cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
        rolling_cutoff = self._rolling_window_start()
        older_apps = (
            self.db.query(PlanningApplication)
            .filter(
                PlanningApplication.council_id == council_id,
                PlanningApplication.created_at >= older_cutoff,
                PlanningApplication.created_at < rolling_cutoff,
            )
            .all()
        )
        if older_apps:
            total_older = len(older_apps) * len(_APPLICATION_KEY_FIELDS)
            populated_older = 0
            for app in older_apps:
                for field in _APPLICATION_KEY_FIELDS:
                    value = getattr(app, field, None)
                    if value is not None and value != "" and value != "Unknown":
                        populated_older += 1
            quality_older = populated_older / total_older * 100
            if quality_older > 0 and quality_now < quality_older * 0.8:
                anomalies.append(
                    f"Data quality dropped from {quality_older:.1f}% to "
                    f"{quality_now:.1f}%. Previously populated fields are now missing."
                )

        if anomalies:
            log.warning(
                "anomalies_detected",
                council_id=council_id,
                anomalies=anomalies,
            )

        return anomalies

    def generate_health_report(self) -> dict[str, Any]:
        """System-wide health report with per-council breakdown."""
        all_health = self.get_all_health()
        failing = [
            h
            for h in all_health
            if h["success_rate"] < _FAILURE_SUCCESS_RATE_THRESHOLD * 100
            or h["consecutive_failures"] > _FAILURE_CONSECUTIVE_THRESHOLD
        ]
        healthy = [h for h in all_health if h not in failing]

        # Anomalies per council
        anomaly_map: dict[int, list[str]] = {}
        for h in all_health:
            cid = h["council_id"]
            anomalies = self.detect_anomalies(cid)
            if anomalies:
                anomaly_map[cid] = anomalies

        avg_success = (
            sum(h["success_rate"] for h in all_health) / len(all_health)
            if all_health
            else 0.0
        )
        avg_quality = (
            sum(h["data_quality_score"] for h in all_health) / len(all_health)
            if all_health
            else 0.0
        )

        report = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "summary": {
                "total_councils_monitored": len(all_health),
                "healthy_councils": len(healthy),
                "failing_councils": len(failing),
                "councils_with_anomalies": len(anomaly_map),
                "average_success_rate": round(avg_success, 2),
                "average_data_quality": round(avg_quality, 2),
            },
            "failing_scrapers": failing,
            "anomalies": anomaly_map,
            "all_councils": all_health,
        }

        log.info(
            "health_report_generated",
            healthy=len(healthy),
            failing=len(failing),
            anomalies=len(anomaly_map),
        )
        return report
