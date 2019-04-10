from datetime import date
from typing import Optional

from common.job_signature import JobSignature
from common.util import convert_class_with_props_to_str
from sweep_builder.data_containers.entity_node import EntityNode


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

    entity_id: str
    entity_type: str
    report_type: str
    job_signature: JobSignature

    ad_account_id: str = None
    timezone: str = None
    entity_hierarchy: EntityNode = None
    range_start: date = None
    report_variant: str = None

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        report_type: str,
        job_signature: JobSignature,
        *,
        ad_account_id: str = None,
        timezone: str = None,
        entity_hierarchy: EntityNode = None,
        range_start: date = None,
        report_variant: str = None,
    ):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.report_type = report_type
        self.job_signature = job_signature
        self.ad_account_id = ad_account_id
        self.timezone = timezone
        self.entity_hierarchy = entity_hierarchy
        self.range_start = range_start
        self.report_variant = report_variant

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __repr__(self):
        return convert_class_with_props_to_str(self)

    @property
    def is_divisible(self) -> bool:
        """Can this task be divided into subtasks."""
        return bool(self.entity_hierarchy)

    @property
    def job_id(self) -> Optional[str]:
        return self.job_signature.job_id
