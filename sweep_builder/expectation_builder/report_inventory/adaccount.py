from typing import Generator

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id

from sweep_builder.expectation_builder.expectation_claim import ExpectationClaim

from common.job_signature import JobSignature


def ad_accounts_per_scope(scope):
    # type: (str) -> Generator[ExpectationClaim]
    """
    WIP

    Generates "fetch AAs entity metadata per given scope" job ID

    To be used by Scope-level RealityClaim / ExpectationClaim.

    :param str scope:
    :rtype: Generator[JobSignature]
    """
    yield ExpectationClaim(
        normative_job_signature = JobSignature(
            generate_id(
                # Note absence of value for AdAccount
                # This is "all AA per scope X" job.
                report_type=ReportType.entity,
                report_variant=Entity.AdAccount,
                range_start=scope
            )
        )
    )
