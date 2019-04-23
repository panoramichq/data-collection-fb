from typing import Optional

from common.job_signature import JobSignature


class PrioritizationClaim:
    """
    Used to express a bundle of data representing scored realization of
    need to have certain data point filled.

    Think of it as "context" object - a dumping ground of data pertinent to entity's existence

    (Used to avoid the need to change all functions in the stack if you need
    to add more data to context from the very bottom of the stack. Just extend this object.)
    """

    entity_id: str
    entity_type: str
    report_type: str
    job_signature: JobSignature
    score: int

    ad_account_id: Optional[str]
    timezone: Optional[str]

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        report_type: str,
        job_signature: JobSignature,
        score: int,
        *,
        ad_account_id: str = None,
        timezone: str = None,
    ):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.report_type = report_type
        self.job_signature = job_signature
        self.score = score
        self.ad_account_id = ad_account_id
        self.timezone = timezone

    @property
    def job_id(self) -> str:
        return self.job_signature.job_id
