from datetime import date
from typing import Optional

from common.job_signature import JobSignature
from common.store.jobreport import JobReport


class ScorableClaim:
    """Expectation claim ready for scoring."""

    entity_id: str
    entity_type: str
    report_type: str
    job_signature: JobSignature
    last_report: Optional[JobReport]

    ad_account_id: Optional[str]
    timezone: Optional[str]
    range_start: date = None
    report_variant: str

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        report_type: str,
        report_variant: str,
        job_signature: JobSignature,
        last_report: Optional[JobReport],
        *,
        ad_account_id: str = None,
        timezone: str = None,
        range_start: date = None,
    ):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.report_type = report_type
        self.report_variant = report_variant
        self.job_signature = job_signature
        self.last_report = last_report
        self.ad_account_id = ad_account_id
        self.timezone = timezone
        self.range_start = range_start

    @property
    def selected_job_id(self) -> str:
        return self.job_signature.job_id

    @property
    def is_first_attempt(self) -> bool:
        """First attempt when no pre-existing job report."""
        return self.last_report is None or self.last_report.last_success_dt is None
