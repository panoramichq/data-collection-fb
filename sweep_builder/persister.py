import logging
import time

from typing import Generator

from common.enums.jobtype import detect_job_type
from common.measurement import Measure
from common.enums.entity import Entity
from oozer.common.expecations_store import JobExpectationsWriter
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper

from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim

logger = logging.getLogger(__name__)

# :) Guess what for
FIRST = 0
LAST = -1

subject_to_expectation_publication = {Entity.Campaign, Entity.AdSet, Entity.Ad}


def should_persist(job_score: int) -> bool:
    """Determine whether job with score should be persisted."""
    return job_score > JobGateKeeper.JOB_NOT_PASSED_SCORE


def iter_persist_prioritized(
    sweep_id: str, prioritized_iter: Generator[PrioritizationClaim, None, None]
) -> Generator[PrioritizationClaim, None, None]:
    """
    Persist prioritized jobs and pass-through context objects for inspection
    """
    # cache_max_size allows us to avoid writing same score
    # for same jobID when given objects rely on same JobID
    # for collection.

    with SortedJobsQueue(sweep_id).JobsWriter() as add_to_queue, JobExpectationsWriter(
        sweep_id, cache_max_size=200000
    ) as expectation_add:

        _measurement_name_base = f'{__name__}.{iter_persist_prioritized.__name__}'
        _measurement_sample_rate = 1

        _before_next_prioritized = time.time()
        skipped_jobs = {}
        for prioritization_claim in prioritized_iter:
            job_type = detect_job_type(prioritization_claim.report_type, prioritization_claim.entity_type)
            _measurement_tags = {
                'entity_type': prioritization_claim.entity_type,
                'report_type': prioritization_claim.report_type,
                'ad_account_id': prioritization_claim.ad_account_id,
                'job_type': job_type,
                'sweep_id': sweep_id,
            }

            Measure.timing(
                f'{_measurement_name_base}.next_prioritized',
                tags=_measurement_tags,
                sample_rate=_measurement_sample_rate,
            )((time.time() - _before_next_prioritized) * 1000)

            # Approaches to Job queueing:

            # * Dire-singles-until-wholesale *
            # Strategy is to allow per-entity jobs with higher score to
            # run first, and if an effectively per-parent job comes by,
            # let it out too. After that, do not let out any further per-entity jobs
            # for the same per-parent.
            # This means that we may be collecting for some of same
            # exact normative job twice - once per own normative score, and
            # then with effective per-parent task.

            # * Dire-singles-until-wholesale-or-speculative tail *
            # Starts like in Dire-singles-until-wholesale above, but
            # Looper will try to track each normative task and subtract these from
            # a list of normatives per effective job parent. If Effective task has
            # not much left to collect for (just a handful of normative tasks left under it)
            # Looper will skip per-parent effective task and will continue letting out
            # per-entity "normative" tasks.

            # * Singles-as-pool-vs-wholesale *
            # Write out all "normative" singles jobs with their scores.
            # Write out all score variants for "effective" wholesale
            # Look ahead of time at entire pool of single normative jobs,
            # find the intersection point and angle of score planes between
            # normative and effective jobs (effective will be flat,
            # normative plain will be at angle. More drastic the angle,
            # the more likely normative pool will be started.

            # * The lazy way *
            # Only the most efficient "effective" job type is queued up
            # Normative jobs are pinned on the effective job as indicators of
            # what normative jobs that "effective" job is responsible for
            # bat normative jobs don't show up anywhere in the queue.
            # There is nothing for Looper / Prioritizer to choose from
            # when it comes to *alternative* ways of getting same data.
            # There is only one way - hardcoded way.

            # Below we are doing it the Lazy way

            # Prioritizer makes sure that the last mention of common
            # (multiple Entities trigger same per-parent)
            # "effective" jobs that are accompanying the normative jobs
            # is marked with highest score computed for that shared "effective"
            # job over all inferences / evaluations of that job needing to exist.
            # In other words, for series of C IDs 1, 2, 3 under AA7,
            # if during C1 estimation Cs-per-AA7 job got individual score 20,
            # during C2 estimation same Cs-per-AA7 job got individual score 40,
            # and during C3 estimation same Cs-per-AA7 job got individual score 30,
            # despite the separate scores, the
            # prioritizer will keep the highest rolling score for each
            # sighting / yielding of Cs-per-AA7 job, resulting in consequitive
            # scores 20, 40, 40 (not 30).
            # Thus, our job here is to save the last score for same repeating
            # "effective" job. However, since we don't know which one is last one,
            # we have to save the score for every signing of Cs-per-AA7, knowing
            # that whichever ends up being last, is guaranteed to be the highest.

            # again, per earlier write up,
            # below is the Lazy way of scheduling job variants
            # We pick most abstract of "effective" job variants
            # which is usually, all x per AA ID approach.
            # At this point (i know because I wrote them 30 minutes ago)
            # we have at most 1 "effective" job variant along with "normative" per
            # prioritization_claim.
            # So, the task becomes "pick the last job variant in line"
            # and register that.
            # We will also steal highest score from whatever all job variants we have.
            # This way no matter what is prioritized first, normative or effective
            # effetive job variant with highest score moves to queue.
            # This logic must obviously change once we are out of proof of concept stage.

            # don't care about JobSignature's args, kwargs at this point
            score_job_id_pairs = [
                (score, job_id) for score, (job_id, job_args, job_kwargs) in prioritization_claim.score_job_pairs
            ]

            # as mentioned earlier, at this time we expect at most 2 job variants
            #  - normative (always present) (data per E ID or data per Es per AA ID)
            #  - effective (optional) (blah per AA)
            _, job_id_effective = score_job_id_pairs[LAST]
            score = max(score for score, _ in score_job_id_pairs)

            if not should_persist(score):
                logger.info(f'Not persisting job {job_id_effective} due to low score: {score}')
                ad_account_id = prioritization_claim.ad_account_id
                if job_type not in skipped_jobs:
                    skipped_jobs[job_type] = {}
                if ad_account_id not in skipped_jobs[job_type]:
                    skipped_jobs[job_type][ad_account_id] = {}
                if prioritization_claim.report_type not in skipped_jobs[job_type][ad_account_id]:
                    skipped_jobs[job_type][ad_account_id][prioritization_claim.report_type] = 0

                skipped_jobs[job_type][ad_account_id][prioritization_claim.report_type] += 1
                continue

            # Following are JobScope attributes we don't store on JobID
            # so we need to store them separately.
            # See JobScope object for exact attr names.
            # At this point persister forms the
            # auxiliary data blob for saving on Data Flower.
            # We don't have to do that here.
            # It can be pre-computed and placed on the JobSignature
            # TODO: contemplate moving auxiliary data formation to
            #       place where JobSignatures are generated and use that
            #       data for Data Flower (as it was originally intended
            #       but not implemented because saving each job's data
            #       individually to Data Flower was too slow)
            # So, here you would unpack
            # **job_kwargs
            # that you get from prioritization_claim.score_job_pairs
            # ... Until then:
            extra_data = {}
            if prioritization_claim.timezone:
                extra_data['ad_account_timezone_name'] = prioritization_claim.timezone

            # we are adding only per-parent job to the queue
            with Measure.timer(
                f'{_measurement_name_base}.add_to_queue', tags=_measurement_tags, sample_rate=_measurement_sample_rate
            ):
                Measure.counter(f'{_measurement_name_base}.add_to_queue_cnt', tags=_measurement_tags).increment()
                add_to_queue(job_id_effective, score, **extra_data)

            # This is our cheap way of ensuring that we are dealing
            # with platform-bound job that we need to report our expectations for
            is_subject_to_expectation_publication = (
                prioritization_claim.ad_account_id is not None
                and prioritization_claim.entity_id is not None
                and prioritization_claim.entity_type in subject_to_expectation_publication
            )

            if is_subject_to_expectation_publication:
                # For purpose of accounting for expectations,
                # must save the normative job to right table
                # Normative job_id comes first score_job_id_pairs
                _, job_id_normative = score_job_id_pairs[FIRST]

                # TODO: contemplate parsing these instead and making sure they are norm vs eff
                # at this point all this checks is that we have more than one job_id scheduled
                if job_id_normative != job_id_effective:
                    with Measure.timer(
                        f'{_measurement_name_base}.expectation_add',
                        tags=_measurement_tags,
                        sample_rate=_measurement_sample_rate,
                    ):
                        expectation_add(
                            job_id_effective, prioritization_claim.ad_account_id, prioritization_claim.entity_id
                        )

            # This time includes the time consumer of this generator wastes
            # between reads from us. Good way to measure how quickly we are
            # consumed (what pauses we have between each consumption)
            with Measure.timer(
                f'{_measurement_name_base}.yield_result', tags=_measurement_tags, sample_rate=_measurement_sample_rate
            ):
                yield prioritization_claim

            _before_next_prioritized = time.time()

        if skipped_jobs:
            for job_type in skipped_jobs:
                for ad_account_id in skipped_jobs[job_type]:
                    for report_type in skipped_jobs[job_type][ad_account_id]:
                        measurement_tags = {
                            'sweep_id': sweep_id,
                            'ad_account_id': ad_account_id,
                            'job_type': job_type,
                            'report_type': report_type,
                        }
                        Measure.gauge(f'{_measurement_name_base}.gatekeeper_stop_jobs', tags=measurement_tags)(
                            skipped_jobs[ad_account_id]
                        )
