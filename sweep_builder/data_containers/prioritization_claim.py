from typing import List, Iterable, Tuple
from itertools import zip_longest

from common.job_signature import JobSignature
from sweep_builder.data_containers.expectation_claim import ExpectationClaim


class PrioritizationClaim(ExpectationClaim):
    """
    Used to express a bundle of data representing scored realization of
    need to have certain data point filled.

    Think of it as "context" object - a dumping ground of data pertinent to entity's existence

    (Used to avoid the need to change all functions in the stack if you need
    to add more data to context from the very bottom of the stack. Just extend this object.)
    """
    # structure matching job_signatures on underlying class
    # here score for each element in original list is matched in
    # position in job_scores list.
    job_scores: List[int] = []

    @property
    def score_job_pairs(self) -> Iterable[Tuple[int, JobSignature]]:
        return zip_longest(self.job_scores, self.job_signatures)
