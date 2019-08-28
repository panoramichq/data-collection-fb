import logging
from datetime import timedelta, datetime
from typing import Tuple, Optional

from common import tztools
from common.connect.redis import get_redis
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

    @classmethod
    def shall_pass(cls, claim: ScorableClaim) -> bool:
        """Return true if job should be re-collected."""
        shall_pass, time_period = cls._shall_pass(claim)
        if not shall_pass and time_period is not None and time_period.total_seconds() > 1:
            JobGateKeeperCache.store_result(claim.job_id, time_period)

        return shall_pass

    @classmethod
    def _shall_pass(cls, claim: ScorableClaim) -> Tuple[bool, Optional[timedelta]]:
        report_type = claim.report_type
        last_progress_dt = None if claim.last_report is None else claim.last_report.last_progress_dt
        last_success_dt = None if claim.last_report is None else claim.last_report.last_success_dt
        # never collected before so you have to try to collect it
        if last_success_dt is None and last_progress_dt is None:
            logger.warning(f'[never-collected] Job {claim.job_id} was never collected yet.')
            return True, None

        if last_progress_dt is not None:
            shall_pass, expected_time = JobGateKeeper._every_x_minutes(
                last_progress_dt, REPORT_IN_PROGRESS_FREQUENCY_MINS
            )
            if not shall_pass or last_success_dt is None:
                # Either task already running or never succeeded
                logger.warning(f'[in-progress] Job {claim.job_id} is in-progress. Gatekeeper result: {shall_pass}')
                return shall_pass, expected_time

        # entity collection every 2 hours (and comments every 4 hours)
        if report_type == ReportType.entity:
            return cls._shall_pass_entity_jobs(claim)
        elif report_type in ReportType.ALL_METRICS:
            return cls._shall_pass_metrics_jobs(claim)
        elif report_type in ReportType.MUST_RUN_EVERY_SWEEP:
            return True, None

        # we dont know what this job is
        raise ValueError(f'Trying to run unknown report type "{report_type}"')

    @classmethod
    def _shall_pass_metrics_jobs(cls, claim: ScorableClaim) -> Tuple[bool, Optional[timedelta]]:
        report_type = claim.report_type
        report_variant = claim.report_variant
        report_day = claim.range_start

        # lifetime data should be attempted to collect at least 6 hours apart
        if report_type == ReportType.lifetime:
            if report_variant == Entity.PageVideo:
                return JobGateKeeper._every_x_hours(
                    claim.last_report.last_success_dt, REPORT_TYPE_LIFETIME_PAGE_VIDEOS_FREQUENCY
                )
            elif report_variant in {Entity.PagePost, Entity.PagePostPromotable}:
                return JobGateKeeper._every_x_hours(
                    claim.last_report.last_success_dt, REPORT_TYPE_LIFETIME_PAGE_POSTS_FREQUENCY
                )
            return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, REPORT_TYPE_LIFETIME_FREQUENCY)

        # just in case we generate job without range_end, we better let it go :)
        if report_day is None:
            return True, None
        datapoint_age_in_days = (now().date() - report_day).total_seconds() / (60 * 60 * 24)

        if report_type in [
            ReportType.day_dma,
            ReportType.day_age_gender,
            ReportType.day_region,
            ReportType.day_country,
        ]:
            if datapoint_age_in_days < 3:
                return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, 24)
            elif datapoint_age_in_days < 14:
                return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, 24 * 3)
        else:
            if datapoint_age_in_days < 7:
                return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, 3)
            elif datapoint_age_in_days < 14:
                return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, 10)
            elif datapoint_age_in_days < 30:
                return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, 24)
            elif datapoint_age_in_days < 90:
                return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, 24 * 7)

        return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, 24 * 7 * 4 * 4)  #  4 Months

    @classmethod
    def _shall_pass_entity_jobs(cls, claim: ScorableClaim) -> Tuple[bool, Optional[timedelta]]:
        if claim.report_variant == Entity.Comment:
            return JobGateKeeper._every_x_hours(
                claim.last_report.last_success_dt, REPORT_TYPE_ENTITY_COMMENTS_FREQUENCY
            )

        return JobGateKeeper._every_x_hours(claim.last_report.last_success_dt, REPORT_TYPE_ENTITY_FREQUENCY)

    @staticmethod
    def _every_x_minutes(last_success_dt: datetime, x_minutes: int) -> Tuple[bool, timedelta]:
        minutes_since_success = (tztools.now() - last_success_dt).total_seconds() / 60
        delta_since_success = tztools.now() - last_success_dt
        return minutes_since_success > x_minutes, timedelta(minutes=x_minutes) - delta_since_success

    @staticmethod
    def _every_x_hours(last_success_dt: datetime, x_hours: int) -> Tuple[bool, timedelta]:
        minutes_since_success = (tztools.now() - last_success_dt).total_seconds() / 60
        delta_since_success = tztools.now() - last_success_dt
        return minutes_since_success > 60 * x_hours, timedelta(hours=x_hours) - delta_since_success


class JobGateKeeperCache:

    """Caches gatekeeper results with TTL."""

    JOB_NOT_PASSED_SCORE = 2

    @staticmethod
    def _gen_job_key(job_id: str) -> str:
        """Generate redis key that works across sweeps."""
        return f'fb:gatekeeper-cache-{job_id}'

    @classmethod
    def shall_pass(cls, job_id: str) -> bool:
        """Check if job shall pass based on cached value."""
        return get_redis().get(cls._gen_job_key(job_id)) is None

    @classmethod
    def store_result(cls, job_id: str, time_period: timedelta):
        """Store entry in cache with suggested TTL."""
        get_redis().setex(cls._gen_job_key(job_id), 1, time_period)
