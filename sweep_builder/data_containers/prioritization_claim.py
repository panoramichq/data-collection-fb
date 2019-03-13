from typing import List, Iterable, Tuple
from itertools import zip_longest

from common.job_signature import JobSignature
from common.enums.entity import Entity
from sweep_builder.data_containers.scorable_claim import ScorableClaim

SUBJECT_TO_EXPECTATION_PUBLICATION = {Entity.Campaign, Entity.AdSet, Entity.Ad}


class PrioritizationClaim(ScorableClaim):
    """
    Used to express a bundle of data representing scored realization of
    need to have certain data point filled.

    Think of it as "context" object - a dumping ground of data pertinent to entity's existence

    (Used to avoid the need to change all functions in the stack if you need
    to add more data to context from the very bottom of the stack. Just extend this object.)
    """
    job_scores: List[int] = []
    score: int = None

    @property
    def score_job_pairs(self) -> Iterable[Tuple[int, JobSignature]]:
        return zip_longest(self.job_scores, self.job_signatures)

    @property
    def is_subject_to_expectation_publication(self):
        return (
            self.ad_account_id is not None and self.entity_id is not None and
            self.entity_type in SUBJECT_TO_EXPECTATION_PUBLICATION
        )
