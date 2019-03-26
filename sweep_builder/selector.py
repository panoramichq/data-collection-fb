import functools

from collections import defaultdict
from typing import Iterable, Generator, Optional

from pynamodb.exceptions import DoesNotExist

from config.jobs import FAILS_IN_ROW_BREAKDOWN_LIMIT
from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.measurement import Measure
from common.store.jobreport import JobReport
from common.id_tools import generate_id
from common.job_signature import JobSignature
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim


# TODO: add maxsize to cache below?
@functools.lru_cache(maxsize=None)
def _fetch_job_report(job_id: str) -> Optional[JobReport]:
    """Retrieve job report from job report table (cached)."""
    try:
        return JobReport.get(job_id)
    except DoesNotExist:
        return None


def generate_child_claims(claim: ExpectationClaim) -> Generator[ExpectationClaim, None, None]:
    entity_type = claim.entity_type or Entity.AdAccount
    child_entity_type = Entity.next_level(entity_type)
    for child_entity_id, child_entity_id_map in claim.entity_id_map.items():
        yield ExpectationClaim(
            child_entity_id,
            child_entity_type,
            ad_account_id=claim.ad_account_id,
            timezone=claim.timezone,
            entity_id_map=child_entity_id_map,
            normative_job_signature=JobSignature(
                generate_id(
                    ad_account_id=claim.ad_account_id,
                    range_start=claim.range_start,
                    report_type=claim.report_type,
                    report_variant=claim.report_variant,
                    entity_id=child_entity_id,
                    entity_type=child_entity_type,
                )
            ),
        )


def should_select(report: JobReport) -> bool:
    """Decide if signature should be used based on last report."""
    # only break down jobs with too large error
    if report.last_failure_bucket != FailureBucket.TooLarge:
        return True

    # need to fail n-times in a row
    if report.fails_in_row is None or report.fails_in_row < FAILS_IN_ROW_BREAKDOWN_LIMIT:
        return True

    return False


def select_signature(claim: ExpectationClaim) -> Generator[ScorableClaim, None, None]:
    """Select job signature for single expectation claim."""
    selected_signature = (
        claim.effective_job_signature if claim.effective_job_signature is not None else claim.normative_job_signature
    )

    last_report = _fetch_job_report(selected_signature.job_id)
    if not claim.is_divisible or last_report is None or should_select(last_report):
        yield ScorableClaim(
            claim.entity_id,
            claim.entity_type,
            selected_signature,
            claim.normative_job_signature,
            last_report,
            ad_account_id=claim.ad_account_id,
            timezone=claim.timezone,
        )
        return

    # break down into smaller jobs recursively
    for child_claim in generate_child_claims(claim):
        yield from select_signature(child_claim)


def iter_select_signature(claims: Iterable[ExpectationClaim]) -> Generator[ScorableClaim, None, None]:
    """Select signature for each expectation claim based on job history."""
    histogram_counter = defaultdict(int)
    for claim in claims:
        for scorable_claim in select_signature(claim):
            histogram_counter[(claim.ad_account_id, claim.entity_type)] += 1
            yield scorable_claim

    for ((ad_account_id, entity_type), count) in histogram_counter.items():
        Measure.histogram(
            f'{__name__}.{iter_select_signature.__name__}.scorable_claims_per_expectation_claim',
            tags={'ad_account_id': ad_account_id, 'entity_type': entity_type},
        )(count)
