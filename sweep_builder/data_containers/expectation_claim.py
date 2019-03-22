from datetime import date
from typing import Any, Dict

from common.job_signature import JobSignature


class ExpectationClaim:
    """
    Used to express a bundle of data representing realization of
    need to have certain data point filled.

    It contains data about underlying entity and record (if present)
    of prior attempts of collection per

    Think of it as "context" object - a dumping ground of data pertinent to entity's existence

    (Used to avoid the need to change all functions in the stack if you need
    to add more data to context from the very bottom of the stack. Just extend this object.)
    """

    # Keeping signatures for lifetime reports (only ones using effective)
    normative_job_signature: JobSignature
    effective_job_signature: JobSignature

    entity_id: str
    entity_type: str
    ad_account_id: str
    timezone: str

    entity_id_map: Dict[str, Any]
    range_start: date
    report_type: str
    report_variant: str

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        ad_account_id: str = None,
        timezone: str = None,
        normative_job_signature: JobSignature = None,
        effective_job_signature: JobSignature = None,
        entity_id_map: Dict[str, Any] = None,
        range_start: str = None,
        report_type: str = None,
        report_variant: str = None,
    ):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.ad_account_id = ad_account_id
        self.timezone = timezone
        self.normative_job_signature = normative_job_signature
        self.effective_job_signature = effective_job_signature
        self.entity_id_map = entity_id_map
        self.range_start = range_start
        self.report_type = report_type
        self.report_variant = report_variant

    @property
    def is_divisible(self):
        return self.entity_id_map is not None

    @property
    def normative_job_id(self):
        return None if self.normative_job_signature is None else self.normative_job_signature.job_id
