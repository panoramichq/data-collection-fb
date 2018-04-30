import logging
import itertools

from celery import chord, group
from datetime import datetime

from common.celeryapp import get_celery_app, RoutingKey
from common.enums.entity import Entity
from common.measurement import Measure
from sweep_builder.data_containers.reality_claim import RealityClaim

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task(routing_key=RoutingKey.longrunning)
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def echo(message='This is Long-Running queue'):
    print(message)


@app.task(routing_key=RoutingKey.longrunning)
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def sweep_builder_task(sweep_id=None, start_looper=True):

    from oozer.tasks import sweep_looper_task

    from .init_tokens import init_tokens
    from .pipeline import iter_pipeline
    from .reality_inferrer.reality import iter_reality_base

    sweep_id = sweep_id or datetime.utcnow().strftime('%Y%m%d%H%M%S')

    # In the jobs persister we purposefully avoid persisting
    # anything besides the Job ID. This means that things like tokens
    # and other data on *Claim is lost.
    # As long as we are doing that, we need to leave tokens somewhere
    # for workers to pick up.
    init_tokens(sweep_id)

    logger.info(f"#{sweep_id} Starting sweep")

    delayed_tasks = []

    for reality_claim in iter_reality_base():
        # what we get here are Scope and AdAccount objects.
        # Children of AdAccount reality claims are to be processed
        # in separate Celery tasks. But we still have jobs
        # associated with Scopes objects, so
        # need to rate and store the jobs before chipping off
        # a separate task for each of AdAccounts.
        if reality_claim.entity_type == Entity.AdAccount:
            delayed_tasks.append(
                # we are using Celery chord to process AdAccounts in parallel
                # for very very large (hundreds of thousands) numbers of AdAccounts,
                # chord management will be super memory expensive,
                # as chord timer/controller will be looking at entire list on
                # each tick.
                # In that case, probably better to switch to
                # a callback per handler + mutex/counter somewhere
                sweep_builder_per_ad_account_task.si(
                    sweep_id,
                    reality_claim
                )
            )
        else:
            cnt = 1
            for claim in iter_pipeline(sweep_id, [reality_claim]):
                cnt += 1
                if cnt % 1000 == 0:
                    logger.info(f'#{sweep_id}-root: Queueing up #{cnt}')

    logger.info(f"#{sweep_id}-root: Queued up a total of {cnt} tasks")

    # see http://docs.celeryproject.org/en/latest/userguide/canvas.html
    # for primer on celery chords/groups
    if start_looper:
        # chains start of sweep looper to moment when all delayed tasks are done
        chord(delayed_tasks, sweep_looper_task.si(sweep_id)).delay()
    else:
        group(delayed_tasks).delay()


@app.task(routing_key=RoutingKey.longrunning, ignore_result=False)
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def sweep_builder_per_ad_account_task(sweep_id, ad_account_reality_claim):
    """

    :param sweep_id:
    :param RealityClaim ad_account_reality_claim:
    :return:
    """
    from .pipeline import iter_pipeline
    from .reality_inferrer.reality import iter_reality_per_ad_account_claim

    reality_claims_iter = itertools.chain(
        [ad_account_reality_claim],
        iter_reality_per_ad_account_claim(ad_account_reality_claim)
    )
    cnt = 0
    for claim in iter_pipeline(sweep_id, reality_claims_iter):
        cnt += 1
        if cnt % 1000 == 0:
            logger.info(f'#{sweep_id}-#{ad_account_reality_claim.ad_account_id}: Queueing up #{cnt}')

    logger.info(f"#{sweep_id}-#{ad_account_reality_claim.ad_account_id}: Queued up a total of {cnt} tasks")

    return cnt
