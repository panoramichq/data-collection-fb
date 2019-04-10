from datetime import datetime
from typing import Optional

from common.enums.reporttype import ReportType
from common.id_tools import JobIdParts
from common.tztools import now
from config.jobs import REPORT_TYPE_ENTITY_FREQUENCY, REPORT_TYPE_LIFETIME_FREQUENCY

SECONDS_IN_DAY = 60 * 60 * 24


class JobGateKeeper:
    """Prevent over-collection of datapoints."""

    LOW_SCORE = 1

    @staticmethod
    def allow_normal_score(job: JobIdParts, last_success_dt: Optional[datetime]):
        """Return true if job should be re-collected."""
        # never collected before so you have to try to collect it
        if last_success_dt is None:
            return True

        minutes_since_success = (now() - last_success_dt).total_seconds() / 60

        # lifetime data should be attempted to collect at least 6 hours apart
        if job.report_type == ReportType.lifetime:
            return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_LIFETIME_FREQUENCY)

        # entity collection every 24 hours
        if job.report_type == ReportType.entity:
            return JobGateKeeper._every_x_hours(minutes_since_success, REPORT_TYPE_ENTITY_FREQUENCY)

        # Single-day jobs have no range_end
        job_range_end = job.range_start if job.range_end is None else job.range_end

        # just in case we generate job without range_end, we better let it go :)
        if job_range_end is None:
            return True
        datapoint_age_in_days = (now().date() - job_range_end).total_seconds() / SECONDS_IN_DAY

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
    def _every_x_hours(minutes_since_success: float, x_hours: int) -> bool:
        return minutes_since_success > 60 * x_hours
