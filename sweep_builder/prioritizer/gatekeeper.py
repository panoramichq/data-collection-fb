from datetime import datetime
from typing import Optional

from common.id_tools import JobIdParts
from common.tztools import now


class JobGateKeeper:
    """Prevent over-collection of datapoints."""

    JOB_NOT_PASSED_SCORE = 1

    @staticmethod
    def shall_pass(job: JobIdParts, last_success_dt: Optional[datetime]):
        """Return true if job should be re-collected."""
        if last_success_dt is None or job.bol is None:
            return True

        # Single-day jobs have no range_end
        job_range_end = job.range_start if job.range_end is None else job.range_end

        minutes_since_success = (now() - last_success_dt).total_seconds() / 60
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
    def _every_x_hours(minutes_since_success, x_hours):
        return minutes_since_success > 60 * x_hours
