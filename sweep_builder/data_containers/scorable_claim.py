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

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        report_type: str,
        # TODO: Rename below
        selected_job_signature: JobSignature,
        last_report: Optional[JobReport],
        *,
        ad_account_id: str = None,
        timezone: str = None,
    ):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.report_type = report_type
        self.job_signature = selected_job_signature
        self.last_report = last_report
        self.ad_account_id = ad_account_id
        self.timezone = timezone

    @property
    def selected_job_id(self) -> str:
        return self.job_signature.job_id
