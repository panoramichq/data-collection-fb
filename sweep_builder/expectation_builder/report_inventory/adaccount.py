from typing import Generator

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id

from sweep_builder.reality_inferrer.reality import RealityClaim
from sweep_builder.expectation_builder.expectation_claim import ExpectationClaim

from common.job_signature import JobSignature


def ad_accounts_per_scope(reality_claim):
    # type: (RealityClaim) -> Generator[ExpectationClaim]
    """
    WIP

    Generates "fetch AAs active entity metadata per given scope" job ID

    To be used by Scope-level RealityClaim / ExpectationClaim.

    :param RealityClaim reality_claim:
    :rtype: Generator[JobSignature]
    """

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures = [
            JobSignature.bind(
                generate_id(
                    # Note absence of value for AdAccount
                    # This is "all AA per scope X" job.
                    report_type=ReportType.console,
                    report_variant=Entity.AdAccount,
                    scope=reality_claim.scope
                ),
            )
        ]
    )
