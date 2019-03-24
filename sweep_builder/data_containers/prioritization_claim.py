from common.enums.entity import Entity
from typing import Optional
from common.job_signature import JobSignature

SUBJECT_TO_EXPECTATION_PUBLICATION = {Entity.Campaign, Entity.AdSet, Entity.Ad}


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
    selected_job_signature: JobSignature

    ad_account_id: str
    score: int = None

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        selected_job_signature: JobSignature,
        normative_job_signature: JobSignature,
        score: int,
        ad_account_id: str = None,
        timezone: str = None,
    ):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.selected_job_signature = selected_job_signature
        self.normative_job_signature = normative_job_signature
        self.score = score
        self.ad_account_id = ad_account_id
        self.timezone = timezone

    @property
    def is_subject_to_expectation_publication(self) -> bool:
        return (
            self.ad_account_id is not None
            and self.entity_id is not None
            and self.entity_type in SUBJECT_TO_EXPECTATION_PUBLICATION
        )

    @property
    def selected_job_id(self) -> str:
        return self.selected_job_signature.job_id

    @property
    def normative_job_id(self) -> Optional[str]:
        return None if self.normative_job_signature is None else self.normative_job_signature.job_id
