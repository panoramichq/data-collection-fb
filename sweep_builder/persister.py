import logging

from typing import Generator, Callable

from oozer.common.sorted_jobs_queue import SortedJobsQueue

from .prioritizer.prioritized import iter_prioritized, PrioritizationClaim


logger = logging.getLogger(__name__)


# :) Guess what for
FIRST = 0
LAST = -1


def iter_persist_prioritized(sweep_id, iter_prioritized=iter_prioritized):
    # type: (str, Callable[..., Generator[PrioritizationClaim]]) -> Generator[PrioritizationClaim]
    """
    Persist prioritized jobs and pass-through context objects for inspection

    :param str sweep_id:
    :param iter_prioritized:
    :type iter_prioritized: () -> Generator[PrioritizationClaim]
    :rtype: Generator[PrioritizationClaim]
    """

    with SortedJobsQueue(sweep_id).JobsWriter() as add_to_queue:

        for prioritization_claim in iter_prioritized():

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
                (score, job_id)
                for score, (job_id, _, _) in prioritization_claim.score_job_pairs
            ]

            # as mentioned earlier, at this time we expect at most 2 job variants
            #  - normative (always present) (data per E ID or data per Es per AA ID)
            #  - effective (optional) (blah per AA)
            _, job_id = score_job_id_pairs[LAST]
            score = max(score for score, _ in score_job_id_pairs)

            add_to_queue(job_id, score)

            yield prioritization_claim
