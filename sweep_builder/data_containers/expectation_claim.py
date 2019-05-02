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
    """ID of entity the report is created for."""
    entity_type: str
    """Type of entity the report is created for."""
    report_type: str
    """Type of report (breakdown type, lifetime, entity collection)."""
    report_variant: str
    """Type of entity the report is reporting on (level in Marketing API)."""
    job_signature: JobSignature
    """Job signature that represents the celery task."""

    ad_account_id: str = None
    """ID of ad account."""
    timezone: str = None
    """Timezone of the ad account."""
    entity_hierarchy: EntityNode = None
    """Used for breaking down tasks into smaller tasks when report size too large."""
    range_start: date = None
    """Date the report is created for."""

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        report_type: str,
        report_variant: str,
        job_signature: JobSignature,
        *,
        ad_account_id: str = None,
        timezone: str = None,
        entity_hierarchy: EntityNode = None,
        range_start: date = None,
    ):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.report_type = report_type
        self.report_variant = report_variant
        self.job_signature = job_signature
        self.ad_account_id = ad_account_id
        self.timezone = timezone
        self.entity_hierarchy = entity_hierarchy
        self.range_start = range_start

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        return convert_class_with_props_to_str(self)

    @property
    def is_divisible(self) -> bool:
        """Can this task be divided into subtasks."""
        return bool(self.entity_hierarchy)

    @property
    def job_id(self) -> Optional[str]:
        return self.job_signature.job_id
