import logging
from datetime import datetime
from typing import Optional

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import JobIdParts
from common.tztools import now
from config.jobs import (
    REPORT_IN_PROGRESS_FREQUENCY_MINS,
    REPORT_TYPE_ENTITY_FREQUENCY,
    REPORT_TYPE_LIFETIME_FREQUENCY,
    REPORT_TYPE_ENTITY_COMMENTS_FREQUENCY,
    REPORT_TYPE_LIFETIME_PAGE_VIDEOS_FREQUENCY,
    REPORT_TYPE_LIFETIME_PAGE_POSTS_FREQUENCY,
)

logger = logging.getLogger(__name__)


class JobGateKeeper:
    """Prevent over-collection of datapoints."""

    JOB_NOT_PASSED_SCORE = 1

    @staticmethod
    def shall_pass(job: JobIdParts, last_success_dt: Optional[datetime], last_progress_dt: Optional[datetime]):
        """Return true if job should be re-collected."""
        # never collected before so you have to try to collect it
        if last_success_dt is None and last_progress_dt is None:
            logger.warning(f'[never-collected] Job {job} was never collected yet.')
            return True

        if last_progress_dt is not None:
            minutes_since_progress = (now() - last_progress_dt).total_seconds() / 60
            shall_pass = JobGateKeeper._every_x_minutes(minutes_since_progress, REPORT_IN_PROGRESS_FREQUENCY_MINS)
            if not shall_pass or last_success_dt is None:
                logger.warning(f'[in-progress] Job {job} is in-progress. Gatekeeper result: {shall_pass}')
                return shall_pass

        minutes_since_success = (now() - last_success_dt).total_seconds() / 60

        # lifetime data should be attempted to collect at least 6 hours apart
        if job.report_type == ReportType.lifetime:
            if job.report_variant == Entity.PageVideo:
                return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_LIFETIME_PAGE_VIDEOS_FREQUENCY)
            elif job.report_variant in {Entity.PagePost, Entity.PagePostPromotable}:
                return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_LIFETIME_PAGE_POSTS_FREQUENCY)
            return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_LIFETIME_FREQUENCY)

        # entity collection every 2 hours (and comments every 4 hours)
        if job.report_type == ReportType.entity:
            if job.report_variant == Entity.Comment:
                return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_ENTITY_COMMENTS_FREQUENCY)

            return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_ENTITY_FREQUENCY)

        # Single-day jobs have no range_end
        job_range_end = job.range_start if job.range_end is None else job.range_end

        # just in case we generate job without range_end, we better let it go :)
        if job_range_end is None:
            return True
        datapoint_age_in_days = (now().date() - job_range_end).total_seconds() / (60 * 60 * 24)

        if datapoint_age_in_days < 3:
            return True
        elif datapoint_age_in_days < 7:
            return JobGateKeeper._every_x_hours(minutes_since_success, 1)
        elif datapoint_age_in_days < 14:
            return JobGateKeeper._every_x_hours(minutes_since_success, 5)
        elif datapoint_age_in_days < 30:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24)
        elif datapoint_age_in_days < 90:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24 * 3)
        else:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24 * 7)

    @staticmethod
    def _every_x_minutes(minutes_since: float, x_minutes: int) -> bool:
        return minutes_since > x_minutes

    @staticmethod
    def _every_x_hours(minutes_since: float, x_hours: int) -> bool:
        return minutes_since > 60 * x_hours
