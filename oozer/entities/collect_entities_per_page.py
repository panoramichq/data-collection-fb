from typing import Generator, Optional, Union, List

from common.enums.entity import Entity
from common.tokens import PlatformTokenManager
from common.id_tools import generate_universal_id
from oozer.common.cold_storage.batch_store import ChunkDumpStore
from oozer.common.vendor_data import add_vendor_data
from oozer.common.facebook_api import (
    PlatformApiContext,
    get_default_page_size,
    get_default_fields,
)
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import (
    ENUM_VALUE_FB_MODEL_MAP,
    FB_PAGE_MODEL,
    FB_PAGE_POST_MODEL,
    ExternalPlatformJobStatus
)
from oozer.entities.feedback_entity_task import feedback_entity_task


def iter_native_entities_per_page(
    page: FB_PAGE_MODEL,
    entity_type: str,
    fields: Optional[List[str]] = None,
    page_size: Optional[int] = None,
) -> Generator[Union[FB_PAGE_POST_MODEL], None, None]:
    """
        Generic getter for entities from the Page edge
    """

    if entity_type not in Entity.NON_AA:
        raise ValueError(
            f'Value of "entity_type" argument must be one of {Entity.NON_AA}, '
            f'got {entity_type} instead.'
        )
    fb_model_klass = ENUM_VALUE_FB_MODEL_MAP[entity_type]

    getter_method = {
        FB_PAGE_POST_MODEL: page.get_posts,
    }[fb_model_klass]

    fields_to_fetch = fields or get_default_fields(fb_model_klass)
    page_size = page_size or get_default_page_size(fb_model_klass)

    params = {
        'summary': False,
    }

    if page_size:
        params['limit'] = page_size

    yield from getter_method(
        fields=fields_to_fetch,
        params=params
    )


def iter_collect_entities_per_page(job_scope: JobScope, _: JobContext):
    """
    Collects an arbitrary entity for a page

    :param JobScope job_scope: The JobScope as we get it from the task itself
    :param JobContext _: A job context we use for entity checksums
    :rtype: Generator[Dict]
    """
    # This handler specifically expects to do per-parent
    # entity fetching, thus requiring proper entity enum in report_variant
    if job_scope.report_variant not in Entity.ALL:
        raise ValueError(
            f"Report level {job_scope.report_variant} specified is not one of supported values: {Entity.ALL}"
        )

    entity_type = job_scope.report_variant

    token = job_scope.token
    if not token:
        raise ValueError(
            f"Job {job_scope.job_id} cannot proceed. No platform tokens provided."
        )

    # We don't use it for getting a token. Something else that calls us does.
    # However, we use it to report usages of the token we got.
    token_manager = PlatformTokenManager.from_job_scope(job_scope)

    with PlatformApiContext(token) as fb_ctx:
        root_fb_entity = fb_ctx.to_fb_model(
            job_scope.ad_account_id, Entity.Page
        )

    entities = iter_native_entities_per_page(
        root_fb_entity,
        entity_type
    )

    record_id_base_data = job_scope.to_dict()
    record_id_base_data.update(
        entity_type=entity_type,
        report_variant=None,
    )

    with ChunkDumpStore(job_scope, chunk_size=200) as store:
        cnt = 0
        for entity in entities:
            entity_data = entity.export_all_data()
            entity_data = add_vendor_data(
                entity_data,
                id=generate_universal_id(
                    entity_id=entity_data.get('id'),
                    **record_id_base_data
                )
            )

            # Store the individual datum, use job context for the cold
            # storage thing to divine whatever it needs from the job context
            store(entity_data)

            # Signal to the system the new entity
            feedback_entity_task.delay(entity_data, entity_type, [None, None])

            yield entity_data
            cnt += 1

            if cnt % 1000 == 0:
                report_job_status_task(
                    ExternalPlatformJobStatus.DataFetched, job_scope
                )
                # default paging size for entities per parent
                # is typically around 25. So, each 100 results
                # means about 4 hits to FB
                token_manager.report_usage(token, 4)

    # Report on the effective task status
    report_job_status_task(
        ExternalPlatformJobStatus.Done, job_scope
    )
    token_manager.report_usage(token)
