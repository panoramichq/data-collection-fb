import logging
import itertools
import time
from typing import Dict

from celery import group

from common.celeryapp import get_celery_app, RoutingKey
from common.enums.entity import Entity
from common.error_inspector import ErrorInspector
from common.measurement import Measure
from oozer.common.task_group import TaskGroup
from sweep_builder.data_containers.reality_claim import RealityClaim

app = get_celery_app()
logger = logging.getLogger(__name__)


def extract_tags_for_build_sweep_slice(
    sweep_id: str, ad_account_reality_claim: RealityClaim, *args, **kwargs
) -> Dict[str, str]:
    return {'sweep_id': sweep_id, 'ad_account_id': ad_account_reality_claim.ad_account_id}


@app.task(routing_key=RoutingKey.longrunning)
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def echo(message: str = 'This is Long-Running queue'):
    print(message)


@app.task(routing_key=RoutingKey.longrunning, ignore_result=False)
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_build_sweep_slice)
@Measure.counter(
    __name__,
    function_name_as_metric=True,
    count_once=True,
    extract_tags_from_arguments=extract_tags_for_build_sweep_slice,
)
def build_sweep_slice_per_ad_account_task(sweep_id: str, ad_account_reality_claim: RealityClaim, task_id: str = None):
    from sweep_builder.pipeline import iter_pipeline

    cnt = 0
    try:
        with TaskGroup.task_context(task_id):

            _measurement_name_base = __name__ + '.' + build_sweep_slice_per_ad_account_task.__name__ + '.'
            _measurement_tags = {'sweep_id': sweep_id, 'ad_account_id': ad_account_reality_claim.ad_account_id}

            reality_claims_iter = itertools.chain([ad_account_reality_claim])

            _step = 1000
            _before_fetch = time.time()
            for claim in iter_pipeline(sweep_id, reality_claims_iter):
                Measure.timing(
                    _measurement_name_base + 'next_persisted',
                    tags={'entity_type': claim.entity_type, **_measurement_tags},
                    sample_rate=0.01,
                )((time.time() - _before_fetch) * 1000)
                cnt += 1

                if cnt % _step == 0:
                    logger.info(f'#{sweep_id}-AA<{ad_account_reality_claim.ad_account_id}>: Queueing up #{cnt}')

                _before_fetch = time.time()

            logger.info(f"#{sweep_id}-AA<{ad_account_reality_claim.ad_account_id}>: Queued up a total of {cnt} tasks")
    except Exception as ex:
        ErrorInspector.inspect(ex, ad_account_reality_claim.ad_account_id, {'sweep_id': sweep_id})

    return cnt


@app.task(routing_key=RoutingKey.longrunning, ignore_result=False)
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_build_sweep_slice)
@Measure.counter(
    __name__,
    function_name_as_metric=True,
    count_once=True,
    extract_tags_from_arguments=extract_tags_for_build_sweep_slice,
)
def build_sweep_slice_per_page(sweep_id: str, page_reality_claim: RealityClaim, task_id: str = None):
    from sweep_builder.pipeline import iter_pipeline
    from sweep_builder.reality_inferrer.reality import iter_reality_per_page_claim

    cnt = 0
    try:
        with TaskGroup.task_context(task_id):

            _measurement_name_base = __name__ + '.' + build_sweep_slice_per_page.__name__ + '.'
            _measurement_tags = {'sweep_id': sweep_id, 'page_id': page_reality_claim.entity_id}

            reality_claims_iter = itertools.chain([page_reality_claim], iter_reality_per_page_claim(page_reality_claim))

            _step = 1000
            _before_fetch = time.time()
            for claim in iter_pipeline(sweep_id, reality_claims_iter):
                Measure.timing(
                    _measurement_name_base + 'next_persisted',
                    tags={'entity_type': claim.entity_type, **_measurement_tags},
                    sample_rate=0.01,
                )((time.time() - _before_fetch) * 1000)
                cnt += 1

                if cnt % _step == 0:
                    logger.info(f'#{sweep_id}-#{page_reality_claim.entity_id}: Queueing up #{cnt}')

                _before_fetch = time.time()

            logger.info(f"#{sweep_id}-#{page_reality_claim.entity_id}: Queued up a total of {cnt} tasks")
    except Exception as ex:
        ErrorInspector.inspect(ex, page_reality_claim.ad_account_id, {'sweep_id': sweep_id})

    return cnt


@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def build_sweep(sweep_id: str):
    from sweep_builder.init_tokens import init_tokens
    from sweep_builder.pipeline import iter_pipeline
    from sweep_builder.reality_inferrer.reality import iter_reality_base

    try:
        _measurement_name_base = __name__ + '.' + build_sweep.__name__ + '.'
        _measurement_tags = {'sweep_id': sweep_id}

        # In the jobs persister we purposefully avoid persisting
        # anything besides the Job ID. This means that things like tokens
        # and other data on *Claim is lost.
        # As long as we are doing that, we need to leave tokens somewhere
        # for workers to pick up.
        logger.info(f"#{sweep_id} Prepositioning platform tokens")
        init_tokens(sweep_id)

        logger.info(f"#{sweep_id} Starting sweep building")

        # task_group = TaskGroup()
        delayed_tasks = []

        cnt = 0
        with Measure.counter(_measurement_name_base + 'outer_loop', tags=_measurement_tags) as cntr:

            for reality_claim in iter_reality_base():
                # what we get here are Scope and AdAccount objects.
                # Children of AdAccount reality claims are to be processed
                # in separate Celery tasks. But we still have jobs
                # associated with Scopes objects, so
                # need to rate and store the jobs before chipping off
                # a separate task for each of AdAccounts.
                if reality_claim.entity_type == Entity.AdAccount:

                    # child_task_id = task_group.generate_task_id()
                    # task_group.report_task_active(child_task_id)

                    delayed_tasks.append(
                        # we are using Celery chord to process AdAccounts in parallel
                        # for very very large (hundreds of thousands) numbers of AdAccounts,
                        # chord management will be super memory expensive,
                        # as chord timer/controller will be looking at entire list on
                        # each tick.
                        # In that case, probably better to switch to
                        # a callback per handler + mutex/counter somewhere
                        build_sweep_slice_per_ad_account_task.si(
                            sweep_id,
                            reality_claim,
                            # task_id=child_task_id
                        )
                    )
                elif reality_claim.entity_type == Entity.Page:
                    delayed_tasks.append(build_sweep_slice_per_page.si(sweep_id, reality_claim))
                else:
                    cnt = 1
                    _step = 1000
                    for _ in iter_pipeline(sweep_id, [reality_claim]):
                        cnt += 1
                        if cnt % _step == 0:
                            cntr += _step
                            logger.info(f'#{sweep_id}-root: Queueing up #{cnt}')

                    # because above counter communicates only increments of _step,
                    # we need to report remainder --- amount under _step
                    cntr += cnt % _step

        logger.info(f"#{sweep_id}-root: Queued up a total of {cnt} tasks")

        # # here we fan out actual work to celery workers
        # # and wait for all tasks to finish before returning
        group_result = group(delayed_tasks).delay()

        # In case the workers crash, go-away (scaling) or are otherwise
        # non-responsive, the following would wait indefinitely.
        # Since that's not desirable and the total sweep build time is minutes at
        # maximum, we add a reasonable timeout
        # Because we are not joining on the results, but actually periodically
        # looking for "you done yet?", we can exit if this threshold is busted, and
        # let the next run recover from the situation
        # You will nee
        should_be_done_by = time.time() + (60 * 20)

        Measure.gauge(f'{_measurement_name_base}per_account_sweep.total', tags=_measurement_tags)(
            len(group_result.results)
        )

        # Monitor the progress. Although this obviously can be achieved with
        # group_result.join(), we need to "see" into the task group progress
        with Measure.gauge(f'{_measurement_name_base}per_account_sweep.done', tags=_measurement_tags) as measure_done:
            while True:
                done_counter = 0
                for result in group_result.results:
                    logger.info(f'{result}: {result.state}')
                    if result.ready():
                        done_counter += 1

                logger.info(f"TOTAL: {done_counter}/{len(group_result.results)}")
                logger.info("=" * 20)

                logger.info("Checking group result")

                measure_done(done_counter)
                if group_result.ready():
                    logger.info(f"#{sweep_id}-root: Sweep build complete")
                    break

                # Important. If we don't sleep, the native join in celery context
                # switches all the time and we end up with 100% cpu, eventually somehow
                # deadlocking the process. 5 seconds is kind of an arbitrary number, but
                # does what we need and the impact of a (potential) delay is absolutely
                # minimal
                time.sleep(5)

                # The last line of defense. Workers did not finish in time we
                # expected, no point waiting, kill it.
                if time.time() > should_be_done_by:
                    Measure.gauge(f'{_measurement_name_base}per_account_sweep.early_exits', tags=_measurement_tags)(1)
                    logger.warning("Exiting incomplete sweep build, it's taking too long")
                    return

        logger.info("Waiting on results join")
        if group_result.supports_native_join:
            group_result.join_native()
        else:
            # Eager mode does not support native join.
            group_result.join()

        # # alternative to Celery's native group_result.join()
        # # our manual task tracking code + join()
        # task_group.join()
        logger.info("Join complete, sweep build ended")
    except Exception as ex:
        ErrorInspector.inspect(ex, None, {'sweep_id': sweep_id})
