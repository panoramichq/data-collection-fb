import logging

from typing import Generator

import config.application

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id
from common.job_signature import JobSignature
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim

logger = logging.getLogger(__name__)


def pages_per_scope(reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    """
    Generates "fetch Pages active entity metadata per given scope" job ID

    To be used by Scope-level RealityClaim / ExpectationClaim.
    """
    yield ExpectationClaim(
        reality_claim.entity_id,
        reality_claim.entity_type,
        ReportType.import_pages,
        Entity.Page,
        JobSignature(
            generate_id(
                namespace=config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE,
                # Note absence of value for Page
                # This is "all Pages per scope X" job.
                entity_id=reality_claim.entity_id,
                entity_type=reality_claim.entity_type,
                report_type=ReportType.import_pages,
                report_variant=Entity.Page,
            )
        ),
    )
