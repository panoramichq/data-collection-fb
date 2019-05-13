import logging

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.tztools import now
from config.jobs import (
    REPORT_IN_PROGRESS_FREQUENCY_MINS,
    REPORT_TYPE_ENTITY_FREQUENCY,
    REPORT_TYPE_LIFETIME_FREQUENCY,
    REPORT_TYPE_ENTITY_COMMENTS_FREQUENCY,
    REPORT_TYPE_LIFETIME_PAGE_VIDEOS_FREQUENCY,
    REPORT_TYPE_LIFETIME_PAGE_POSTS_FREQUENCY,
)
from sweep_builder.data_containers.scorable_claim import ScorableClaim

logger = logging.getLogger(__name__)


class JobGateKeeper:
    """Prevent over-collection of datapoints."""

    JOB_NOT_PASSED_SCORE = 1

    @staticmethod
    def shall_pass(claim: ScorableClaim):
        """Return true if job should be re-collected."""
        report_type = claim.report_type
        report_variant = claim.report_variant
        report_day = claim.range_start
        last_progress_dt = None if claim.last_report is None else claim.last_report.last_progress_dt
        last_success_dt = None if claim.last_report is None else claim.last_report.last_success_dt
        # never collected before so you have to try to collect it
        if last_success_dt is None and last_progress_dt is None:
            logger.warning(f'[never-collected] Job {claim.job_id} was never collected yet.')
            return True

        if last_progress_dt is not None:
            minutes_since_progress = (now() - last_progress_dt).total_seconds() / 60
            shall_pass = JobGateKeeper._every_x_minutes(minutes_since_progress, REPORT_IN_PROGRESS_FREQUENCY_MINS)
            if not shall_pass or last_success_dt is None:
                # Either task already running or never succeeded
                logger.warning(
                    f'[in-progress] Job {claim.job_id} is in-progress. Gatekeeper result: {shall_pass}'
                )
                return shall_pass

        # Task not running and has succeeded before
        minutes_since_success = (now() - last_success_dt).total_seconds() / 60

        # lifetime data should be attempted to collect at least 6 hours apart
        if report_type == ReportType.lifetime:
            if report_variant == Entity.PageVideo:
                return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_LIFETIME_PAGE_VIDEOS_FREQUENCY)
            elif report_variant in {Entity.PagePost, Entity.PagePostPromotable}:
                return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_LIFETIME_PAGE_POSTS_FREQUENCY)
            return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_LIFETIME_FREQUENCY)

        # entity collection every 2 hours (and comments every 4 hours)
        if report_type == ReportType.entity:
            if report_variant == Entity.Comment:
                return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_ENTITY_COMMENTS_FREQUENCY)

            return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_ENTITY_FREQUENCY)

        # just in case we generate job without range_end, we better let it go :)
        if report_day is None:
            return True
        datapoint_age_in_days = (now().date() - report_day).total_seconds() / (60 * 60 * 24)

        if datapoint_age_in_days < 7:
            return JobGateKeeper._every_x_hours(minutes_since_success, 3)
        elif datapoint_age_in_days < 14:
            return JobGateKeeper._every_x_hours(minutes_since_success, 10)
        elif datapoint_age_in_days < 30:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24)
        elif datapoint_age_in_days < 90:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24 * 7)
        else:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24 * 7 * 3)

    @staticmethod
    def _every_x_minutes(minutes_since: float, x_minutes: int) -> bool:
        return minutes_since > x_minutes

    @staticmethod
    def _every_x_hours(minutes_since: float, x_hours: int) -> bool:
        return minutes_since > 60 * x_hours
