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
    if not reality_claim.tokens:
        logger.warning(f"Tokens for Scope '{reality_claim.entity_id}' are missing. Skipping all work for this scope.")
        return

    yield ExpectationClaim(
        reality_claim.to_dict(),
        report_type=ReportType.import_pages,
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    namespace=config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE,
                    # Note absence of value for Page
                    # This is "all Pages per scope X" job.
                    entity_id=reality_claim.entity_id,
                    entity_type=reality_claim.entity_type,
                    report_type=ReportType.import_pages,
                    report_variant=Entity.Page,
                )
            )
        ],
    )


def sync_expectations_per_page(reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    """
    Generates "Communicate all calculated expectation Job IDs to Cold Store" job ID

    To be used by Scope-level RealityClaim / ExpectationClaim.
    """
    if not reality_claim.ad_account_id:
        ValueError("PageID value is missing")

    if reality_claim.entity_type != Entity.Page:
        ValueError("Only Page-level expectations may generate this job signature")

    yield ExpectationClaim(
        reality_claim.to_dict(),
        report_type=ReportType.sync_expectations,
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    entity_id=reality_claim.ad_account_id,
                    entity_type=reality_claim.entity_type,
                    report_type=ReportType.sync_expectations,
                )
            )
        ],
    )
