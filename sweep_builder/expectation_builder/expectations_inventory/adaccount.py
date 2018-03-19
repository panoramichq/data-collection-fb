import logging

from typing import Generator

import config.application

from common.facebook.enums.entity import Entity
from common.facebook.enums.reporttype import ReportType
from common.id_tools import generate_id
from common.job_signature import JobSignature
from sweep_builder.expectation_builder.expectation_claim import ExpectationClaim
from sweep_builder.reality_inferrer.reality import RealityClaim


logger = logging.getLogger(__name__)


def ad_accounts_per_scope(reality_claim):
    # type: (RealityClaim) -> Generator[ExpectationClaim]
    """
    Generates "fetch AAs active entity metadata per given scope" job ID

    To be used by Scope-level RealityClaim / ExpectationClaim.

    :param RealityClaim reality_claim:
    :rtype: Generator[ExpectationClaim]
    """

    if not reality_claim.tokens:
        logger.warning(
            f"Tokens for Scope '{reality_claim.entity_id}' are missing. Skipping all work for this scope."
        )
        return

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures = [
            JobSignature.bind(
                generate_id(
                    namespace=config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE,
                    # Note absence of value for AdAccount
                    # This is "all AA per scope X" job.
                    entity_id=reality_claim.entity_id,
                    entity_type=reality_claim.entity_type,
                    report_type=ReportType.import_accounts,
                    report_variant=Entity.AdAccount
                )
            )
        ]
    )
