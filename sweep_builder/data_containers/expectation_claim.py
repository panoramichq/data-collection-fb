from typing import List, Optional

from common.job_signature import JobSignature

from sweep_builder.data_containers.reality_claim import RealityClaim


class ExpectationClaim(RealityClaim):
    """
    Used to express a bundle of data representing realization of
    need to have certain data point filled.

    It contains data about underlying entity and record (if present)
    of prior attempts of collection per

    Think of it as "context" object - a dumping ground of data pertinent to entity's existence

    (Used to avoid the need to change all functions in the stack if you need
    to add more data to context from the very bottom of the stack. Just extend this object.)
    """

    # see base attrs on RealityClaim.
    # here we just top that off with more

    job_signatures: List[JobSignature] = []

    @property
    def normative_job_signature(self) -> Optional[JobSignature]:
        try:
            return self.job_signatures[0]
        except IndexError:
            return None
