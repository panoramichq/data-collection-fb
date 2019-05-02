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


def ad_accounts_per_scope(reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    """
    Generates "fetch AAs active entity metadata per given scope" job ID

    To be used by Scope-level RealityClaim / ExpectationClaim.
    """
    if not reality_claim.tokens:
        logger.warning(f"Tokens for Scope '{reality_claim.entity_id}' are missing. Skipping all work for this scope.")
        return

    yield ExpectationClaim(
        reality_claim.entity_id,
        reality_claim.entity_type,
        ReportType.import_accounts,
        Entity.AdAccount,
        JobSignature(
            generate_id(
                namespace=config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE,
                # Note absence of value for AdAccount
                # This is "all AA per scope X" job.
                entity_id=reality_claim.entity_id,
                entity_type=reality_claim.entity_type,
                report_type=ReportType.import_accounts,
                report_variant=Entity.AdAccount,
            )
        ),
    )
