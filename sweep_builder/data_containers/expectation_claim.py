from datetime import date
from typing import Any, Dict

from common.enums.entity import Entity
from common.id_tools import generate_id
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

    # Keeping signatures for lifetime reports (only ones using effective)
    normative_job_signature: JobSignature = None
    effective_job_signature: JobSignature = None
    entity_id_map: Dict[str, Any] = {}
    range_start: date = None
    report_type: str = None
    report_variant: str = None

    @property
    def is_dividable(self):
        return self.entity_id_map is not None

    @property
    def normative_job_id(self):
        return None if self.normative_job_signature is None else self.normative_job_signature.job_id

    def generate_child_claims(self):
        entity_type = self.entity_type or Entity.AdAccount
        for child_entity_id, child_entity_id_map in self.entity_id_map.items():
            yield ExpectationClaim(
                # TODO: we need to avoid doing this everywhere
                self.to_dict(),
                entity_id_map=child_entity_id_map,
                normative_job_signature=generate_id(
                    ad_account_id=self.ad_account_id,
                    range_start=self.range_start,
                    report_type=self.report_type,
                    report_variant=self.report_variant,
                    entity_id=child_entity_id,
                    entity_type=Entity.next_level(entity_type)  # TODO: does not have entity type
                )
            )
