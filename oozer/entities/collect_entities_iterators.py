import itertools
from typing import List, Generator, Callable, Dict, Union, Tuple, Any

from common.enums.entity import Entity
from common.id_tools import generate_universal_id, NAMESPACE_RAW
from common.page_tokens import PageTokenManager
from common.tokens import PlatformTokenManager
from oozer.common.cold_storage import ChunkDumpStore
from oozer.common.enum import (
    ENUM_VALUE_FB_MODEL_MAP,
    FB_ADACCOUNT_MODEL,
    FB_CAMPAIGN_MODEL,
    FB_ADSET_MODEL,
    FB_AD_MODEL,
    FB_AD_CREATIVE_MODEL,
    FB_AD_VIDEO_MODEL,
    FB_CUSTOM_AUDIENCE_MODEL,
    FB_PAGE_MODEL,
    FB_PAGE_POST_MODEL,
    FB_COMMENT_MODEL,
    ColdStoreBucketType,
)
from oozer.common.facebook_api import (
    get_default_fields,
    get_default_page_size,
    PlatformApiContext,
    get_additional_params,
)
from oozer.common.job_scope import JobScope
from oozer.common.vendor_data import add_vendor_data
from oozer.entities.feedback_entity_task import feedback_entity_task

DEFAULT_CHUNK_SIZE = 200


def _extract_token_entity_type_parent_entity(
    job_scope: JobScope, allowed_entity_types: List[str], parent_entity_type: str, parent_entity_id_key: str
) -> Tuple[str, str, Any]:
    """
    Returned values here are token, entity_type and initialized parent entity from Facebook SDK
    """
    if job_scope.report_variant not in allowed_entity_types:
        raise ValueError(
            f"Report level {job_scope.report_variant} specified is not one of supported values: {allowed_entity_types}"
        )

    entity_type = job_scope.report_variant
    token = job_scope.token
    if not token:
        raise ValueError(f"Job {job_scope.job_id} cannot proceed. No platform tokens provided.")

    with PlatformApiContext(token) as fb_ctx:
        root_fb_entity = fb_ctx.to_fb_model(job_scope[parent_entity_id_key], parent_entity_type)

    return token, entity_type, root_fb_entity


def _iterate_native_entities_per_parent(
    allowed_entity_types: List[str],
    getter_methods_map: Dict[object, Callable],
    entity_type: str,
    fields: List[str] = None,
    page_size: int = None,
) -> Generator:
    """
    Generic getter for entities from the parent's edge from FB API
    """
    if entity_type not in allowed_entity_types:
        raise ValueError(
            f'Value of "entity_type" argument must be one of {allowed_entity_types}, ' f'got {entity_type} instead.'
        )
    fb_model_klass = ENUM_VALUE_FB_MODEL_MAP[entity_type]

    getter_method = getter_methods_map.get(fb_model_klass)
    if not getter_method:
        raise ValueError(f'Value of "entity_type" argument must be one of does not have getter method.')

    fields_to_fetch = fields or get_default_fields(fb_model_klass)
    page_size = page_size or get_default_page_size(fb_model_klass)
    additional_params = get_additional_params(fb_model_klass)
    params = {'summary': False, **additional_params}

    if page_size:
        params['limit'] = page_size

    yield from getter_method(fields=fields_to_fetch, params=params)


def iter_native_entities_per_adaccount(
    ad_account: FB_ADACCOUNT_MODEL, entity_type: str, fields: List[str] = None, page_size: int = None
) -> Generator[
    Union[
        FB_CAMPAIGN_MODEL,
        FB_ADSET_MODEL,
        FB_AD_MODEL,
        FB_AD_CREATIVE_MODEL,
        FB_AD_VIDEO_MODEL,
        FB_CUSTOM_AUDIENCE_MODEL,
    ],
    None,
    None,
]:
    """
    Generic getter for entities from the AdAccount edge
    """

    def get_augmented_account_ad_videos(fields, params):
        parsed_account_id = ad_account['id'].split('_')[1]  # Parse act_12345678
        for ad_video in ad_account.get_ad_videos(fields=fields, params=params):
            ad_video['account_id'] = parsed_account_id
            yield ad_video

    getter_method_map = {
        FB_CAMPAIGN_MODEL: ad_account.get_campaigns,
        FB_ADSET_MODEL: ad_account.get_ad_sets,
        FB_AD_MODEL: ad_account.get_ads,
        FB_AD_CREATIVE_MODEL: ad_account.get_ad_creatives,
        FB_AD_VIDEO_MODEL: get_augmented_account_ad_videos,
        FB_CUSTOM_AUDIENCE_MODEL: ad_account.get_custom_audiences,
    }

    return _iterate_native_entities_per_parent(Entity.AA_SCOPED, getter_method_map, entity_type, fields, page_size)


def iter_native_entities_per_page(
    page: FB_PAGE_MODEL, entity_type: str, fields: List[str] = None, page_size: int = None
) -> Generator[Union[FB_PAGE_POST_MODEL], None, None]:
    """
    Generic getter for entities from the Page edge
    """
    getter_method_map = {FB_PAGE_POST_MODEL: page.get_posts, FB_AD_VIDEO_MODEL: page.get_videos}

    return _iterate_native_entities_per_parent(
        [Entity.PagePost, Entity.PageVideo], getter_method_map, entity_type, fields, page_size
    )


def _iter_get_promotable_posts(page: FB_PAGE_MODEL):
    def fn(fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        # Call {page-id}/feed and filter by is_eligible_for_promotion field for published Page posts
        for page_post in page.get_feed(fields, params, batch, success, failure, pending):
            if page_post.get('is_eligible_for_promotion'):
                yield page_post

        # Call the {page-id}/ads_posts field for unpublished posts including ads_posts type, which are hidden posts
        # created from the Ads Posts tool in Ads Manager, and inline_create type, which are hidden posts
        # backing published Ads
        for page_post in page.get_ads_posts(fields, {'include_inline_create': True}, batch, success, failure, pending):
            if page_post.get('is_eligible_for_promotion'):
                yield page_post
    return fn


def iter_native_entities_per_page_graph(
    page: FB_PAGE_MODEL, entity_type: str, fields: List[str] = None, page_size: int = None
) -> Generator[Union[FB_PAGE_POST_MODEL], None, None]:
    """
    Generic getter for entities from the Page edge using Graph API
    """
    getter_method_map = {FB_PAGE_POST_MODEL: _iter_get_promotable_posts(page)}

    return _iterate_native_entities_per_parent(
        [Entity.PagePostPromotable], getter_method_map, entity_type, fields, page_size
    )


def iter_native_entities_per_page_post(
    page_post: FB_PAGE_POST_MODEL, entity_type: str, fields: List[str] = None, page_size: int = None
) -> Generator[Union[FB_PAGE_POST_MODEL], None, None]:
    """
    Generic getter for entities from the Page post edge
    """
    getter_method_map = {FB_COMMENT_MODEL: page_post.get_comments}

    return _iterate_native_entities_per_parent([Entity.Comment], getter_method_map, entity_type, fields, page_size)


def iter_collect_entities_per_adaccount(job_scope: JobScope) -> Generator[Dict[str, Any], None, None]:
    """
    Collects an arbitrary entity for an ad account
    """
    token, entity_type, root_fb_entity = _extract_token_entity_type_parent_entity(
        job_scope, Entity.AA_SCOPED, Entity.AdAccount, 'ad_account_id'
    )

    entities = iter_native_entities_per_adaccount(root_fb_entity, entity_type)

    record_id_base_data = job_scope.to_dict()
    record_id_base_data.update(entity_type=entity_type, report_variant=None)

    token_manager = PlatformTokenManager.from_job_scope(job_scope)
    with ChunkDumpStore(job_scope, chunk_size=DEFAULT_CHUNK_SIZE) as store:
        for cnt, entity in enumerate(entities):
            entity_data = entity.export_all_data()
            entity_data = add_vendor_data(
                entity_data,
                id=generate_universal_id(
                    # FIXME: add a bug to facebook ads (get_ad_videos doesnt return ad videos but AbstractCrudObject)
                    # FIXME so it is unable to access entity.Field.id then (only a problem for ad videos)
                    entity_id=entity_data.get('id'),
                    **record_id_base_data,
                ),
            )

            # Store the individual datum, use job context for the cold
            # storage thing to divine whatever it needs from the job context
            store(entity_data)

            # Signal to the system the new entity
            feedback_entity_task.delay(entity_data, entity_type, [None, None])

            yield entity_data

            if cnt % 1000 == 0:
                # default paging size for entities per parent
                # is typically around 200. So, each 200 results
                # means about 5 hits to FB
                token_manager.report_usage(token, 5)

    # Report on the effective task status
    token_manager.report_usage(token)


def _augment_page_post(page_post: Dict[str, Any]) -> Dict[str, Any]:
    """
        Augment page posts to reflect changes in version 3.3
        Once we confirm that all page posts are downloaded using the newest version, we can drop this augmentation
        and perform it in platform views

        Source: https://developers.facebook.com/docs/graph-api/changelog/version3.3
    """
    if 'attachments' in page_post and len(page_post['attachments']['data']) == 1:
        data = page_post['attachments']['data'][0]
        if 'title' in data:
            page_post['caption'] = data['title']
            page_post['name'] = data['title']
        if 'description' in data:
            page_post['description'] = data['description']
        if 'url_unshimmed' in data:
            page_post['link'] = data['url_unshimmed']
        if 'target' in data and 'id' in data['target']:
            page_post['object_id'] = data['target']['id']
        if 'media' in data and 'source' in data['media']:
            page_post['source'] = data['media']['source']
        if 'type' in data:
            page_post['type'] = data['type']

    return page_post


def iter_collect_entities_per_page(job_scope: JobScope) -> Generator[Dict[str, Any], None, None]:
    """
    Collects an arbitrary entity for a page
    """
    token, entity_type, root_fb_entity = _extract_token_entity_type_parent_entity(
        job_scope, [Entity.PagePost, Entity.PageVideo], Entity.Page, 'ad_account_id'
    )

    entities = iter_native_entities_per_page(root_fb_entity, entity_type)

    record_id_base_data = job_scope.to_dict()
    record_id_base_data.update(entity_type=entity_type, report_variant=None)

    token_manager = PlatformTokenManager.from_job_scope(job_scope)
    with ChunkDumpStore(job_scope, chunk_size=DEFAULT_CHUNK_SIZE) as store, ChunkDumpStore(
        job_scope,
        chunk_size=DEFAULT_CHUNK_SIZE,
        bucket_type=ColdStoreBucketType.RAW_BUCKET,
        custom_namespace=NAMESPACE_RAW,
    ) as raw_store:
        cnt = 0
        for entity in entities:
            entity_data = entity.export_all_data()

            entity_data = add_vendor_data(
                entity_data, id=generate_universal_id(entity_id=entity_data.get('id'), **record_id_base_data)
            )
            entity_data['page_id'] = job_scope.ad_account_id

            if entity_type == Entity.PagePost:
                # store raw version of response (just to remain consistent)
                raw_store(entity_data)
                entity_data = _augment_page_post(entity_data)

            # Store the individual datum, use job context for the cold
            # storage thing to divine whatever it needs from the job context
            store(entity_data)

            # Signal to the system the new entity
            feedback_entity_task.delay(entity_data, entity_type, [None, None])

            yield entity_data
            cnt += 1

            if cnt % 1000 == 0:
                # default paging size for entities per parent
                # is typically around 200. So, each 200 results
                # means about 5 hits to FB
                token_manager.report_usage(token, 5)

    token_manager.report_usage(token)


def iter_collect_entities_per_page_graph(job_scope: JobScope) -> Generator[Dict[str, Any], None, None]:
    """
    Collects an arbitrary entity for a page using graph API
    """
    page_token_manager = PageTokenManager.from_job_scope(job_scope)
    with PlatformApiContext(page_token_manager.get_best_token(job_scope.ad_account_id)) as fb_ctx:
        page_root_fb_entity = fb_ctx.to_fb_model(job_scope.ad_account_id, Entity.Page)

    entity_type = job_scope.report_variant
    entities = iter_native_entities_per_page_graph(page_root_fb_entity, entity_type)

    record_id_base_data = job_scope.to_dict()
    record_id_base_data.update(entity_type=entity_type, report_variant=None)

    with ChunkDumpStore(job_scope, chunk_size=DEFAULT_CHUNK_SIZE) as store, ChunkDumpStore(
        job_scope,
        chunk_size=DEFAULT_CHUNK_SIZE,
        bucket_type=ColdStoreBucketType.RAW_BUCKET,
        custom_namespace=NAMESPACE_RAW,
    ) as raw_store:
        for entity in entities:
            entity_data = entity.export_all_data()
            entity_data = add_vendor_data(
                entity_data, id=generate_universal_id(entity_id=entity_data.get('id'), **record_id_base_data)
            )
            entity_data['page_id'] = job_scope.ad_account_id

            if entity_type == Entity.PagePostPromotable:
                # store raw version of response (just to remain consistent)
                raw_store(entity_data)
                entity_data = _augment_page_post(entity_data)

            # Store the individual datum, use job context for the cold
            # storage thing to divine whatever it needs from the job context
            store(entity_data)

            # Signal to the system the new entity
            feedback_entity_task.delay(entity_data, entity_type, [None, None])
            yield entity_data


def iter_collect_entities_per_page_post(job_scope: JobScope) -> Generator[Dict[str, Any], None, None]:
    """
    Collects an arbitrary entity for a page post
    """
    token, entity_type, root_fb_entity = _extract_token_entity_type_parent_entity(
        job_scope, [Entity.Comment], Entity.PagePost, 'entity_id'
    )

    entities = iter_native_entities_per_page_post(root_fb_entity, entity_type)

    record_id_base_data = job_scope.to_dict()
    record_id_base_data.update(entity_type=entity_type, report_variant=None)
    del record_id_base_data['entity_id']

    token_manager = PlatformTokenManager.from_job_scope(job_scope)
    with ChunkDumpStore(job_scope, chunk_size=DEFAULT_CHUNK_SIZE) as store:
        cnt = 0
        for entity in entities:
            entity_data = entity.export_all_data()
            entity_data = add_vendor_data(
                entity_data, id=generate_universal_id(entity_id=entity_data.get('id'), **record_id_base_data)
            )
            entity_data['page_id'] = job_scope.ad_account_id
            entity_data['page_post_id'] = job_scope.entity_id

            # Store the individual datum, use job context for the cold
            # storage thing to divine whatever it needs from the job context
            store(entity_data)

            yield entity_data
            cnt += 1

            if cnt % 1000 == 0:
                # default paging size for entities per parent
                # is typically around 250. So, each 250 results
                # means about 4 hits to FB
                token_manager.report_usage(token, 4)

    token_manager.report_usage(token)
