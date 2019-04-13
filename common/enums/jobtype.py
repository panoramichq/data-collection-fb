from typing import Optional

from common.enums.entity import Entity
from common.enums.reporttype import ReportType


class JobType:

    PAID_DATA = 'paid-data'
    ORGANIC_DATA = 'organic-data'
    GLOBAL = 'global'
    UNKNOWN = 'unknown'


OTHER_JOB_REPORT_TYPES = (
    ReportType.sync_expectations,
    ReportType.sync_status,
    ReportType.import_accounts,
    ReportType.import_pages,
)


def detect_job_type(report_type: Optional[str], report_variant: Optional[str]) -> str:
    if not report_variant and report_type in OTHER_JOB_REPORT_TYPES:
        return JobType.GLOBAL
    elif report_variant in Entity.AA_SCOPED:
        return JobType.PAID_DATA
    elif report_variant in Entity.NON_AA_SCOPED:
        return JobType.ORGANIC_DATA

    return JobType.UNKNOWN
