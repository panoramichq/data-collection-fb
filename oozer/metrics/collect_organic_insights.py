from typing import Dict, Any, Generator, Union

from facebook_business.adobjects.insightsresult import InsightsResult
from facebook_business.api import FacebookRequest

from common.enums.entity import Entity
from common.id_tools import NAMESPACE_RAW
from common.tokens import PlatformTokenManager
from oozer.common.cold_storage import batch_store
from oozer.common.enum import (
    ReportEntityApiKind,
    FB_AD_VIDEO_MODEL,
    ColdStoreBucketType,
    FB_PAGE_MODEL,
    FB_PAGE_POST_MODEL,
)
from oozer.common.facebook_api import PlatformApiContext, DEFAULT_PAGE_ACCESS_TOKEN_LIMIT
from oozer.common.job_scope import JobScope
from oozer.common.vendor_data import add_vendor_data

from oozer.metrics.constants import INSIGHTS_REPORT_FIELDS, ORGANIC_DATA_FIELDS_MAP
from oozer.metrics.vendor_data_extractor import (
    report_type_vendor_data_extractor_map,
    report_type_vendor_data_raw_extractor_map,
    ORGANIC_DATA_ENTITY_ID_MAP,
)


class InsightsOrganic:
    @staticmethod
    def fetch_page_token(fb_ctx: PlatformApiContext, page_id: str) -> str:
        # In ideal world, this logic is moved to the beginning of the pipeline so each task does not need to fetch it
        # Their AdAccountUser object is missing `get_accounts` method

        request = FacebookRequest(node_id='me', method='GET', endpoint='/accounts', api=fb_ctx.api, api_type='NODE')
        request.add_params({'limit': DEFAULT_PAGE_ACCESS_TOKEN_LIMIT})

        while True:
            # I assume that there's a better way to do paginate over this, but I wasn't able to find the corresponding
            # target class in SDK :/
            response = request.execute()
            response_json = response.json()

            selected_pages = [entry for entry in response_json['data'] if entry['id'] == page_id]
            if selected_pages:
                return selected_pages[0]['access_token']

            if 'next' in response_json['paging']:
                request._path = response_json['paging']['next']
            else:
                break

        raise ValueError(f'Cannot generate Page Access token for page_id "{page_id}"')

    @staticmethod
    def _detect_report_api_kind(job_scope: JobScope) -> str:
        if job_scope.entity_type == Entity.Page:
            return ReportEntityApiKind.Page
        elif job_scope.entity_type in {Entity.PagePost, Entity.PagePostPromotable}:
            return ReportEntityApiKind.Post
        elif job_scope.entity_type == Entity.PageVideo:
            return ReportEntityApiKind.Video
        else:
            raise ValueError(f'Unknown entity type "{job_scope.entity_type}" when detecting report API kind')

    @staticmethod
    def iter_video_insights(fb_entity: FB_AD_VIDEO_MODEL) -> Generator[Dict[str, Any], None, None]:
        """
        Run the actual execution of the insights job for video insights
        """

        params = {'metric': ORGANIC_DATA_FIELDS_MAP[ReportEntityApiKind.Video]}
        report_status_obj: InsightsResult = fb_entity.get_video_insights(params=params, fields=INSIGHTS_REPORT_FIELDS)

        for datum in report_status_obj:
            yield datum.export_all_data()

    @staticmethod
    def iter_other_insights(
        fb_entity: Union[FB_PAGE_MODEL, FB_PAGE_POST_MODEL], report_entity_kind: str
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Run the actual execution of the insights job for video insights
        """

        params = {'metric': ORGANIC_DATA_FIELDS_MAP[report_entity_kind]}
        report_status_obj: InsightsResult = fb_entity.get_insights(params=params, fields=INSIGHTS_REPORT_FIELDS)

        for datum in report_status_obj:
            yield datum.export_all_data()

    @classmethod
    def iter_collect_insights(cls, job_scope: JobScope):
        """
        Central, *GENERIC* implementation of insights fetcher task

        The goal of this method is to be the entry point for
        metrics fetching Celery tasks. This method is expected to parse
        the JobScope object, figure out that needs to be done
        based on data in the JobScope object and convert that data into
        proper parameters for calling FB

        :param job_scope: The JobScope as we get it from the task itself
        """
        if not job_scope.tokens:
            raise ValueError(f"Job {job_scope.job_id} cannot proceed. No platform tokens provided.")

        token = job_scope.token
        token_manager = PlatformTokenManager.from_job_scope(job_scope)
        report_entity_kind = InsightsOrganic._detect_report_api_kind(job_scope)

        if report_entity_kind == ReportEntityApiKind.Video:
            with PlatformApiContext(job_scope.token) as fb_ctx:
                report_root_fb_entity = fb_ctx.to_fb_model(job_scope.entity_id, job_scope.report_variant)

            data_iter = cls.iter_video_insights(report_root_fb_entity)

        elif report_entity_kind in {ReportEntityApiKind.Page, ReportEntityApiKind.Post}:
            with PlatformApiContext(job_scope.token) as fb_ctx:
                page_token = InsightsOrganic.fetch_page_token(fb_ctx, job_scope.ad_account_id)

            with PlatformApiContext(page_token) as fb_ctx:
                report_root_fb_entity = fb_ctx.to_fb_model(job_scope.entity_id, job_scope.report_variant)

            data_iter = cls.iter_other_insights(report_root_fb_entity, report_entity_kind)
        else:
            raise ValueError(f'Unsupported report entity kind "{report_entity_kind}" to collect organic insights')

        for datum in cls._iter_collect_organic_insights(data_iter, job_scope):
            yield datum
        # right now, we support fetching insights for only one entity at a time
        # so no reason to report usage here
        token_manager.report_usage(token)

    @classmethod
    def _iter_collect_organic_insights(
        cls, data_iter: Generator[Dict[str, Any], None, None], job_scope: JobScope
    ) -> Generator[Dict[str, Any], None, None]:
        raw_store = batch_store.NormalStore(
            job_scope, bucket_type=ColdStoreBucketType.RAW_BUCKET, custom_namespace=NAMESPACE_RAW
        )
        orig_store = batch_store.NormalStore(job_scope, bucket_type=ColdStoreBucketType.ORIGINAL_BUCKET)
        common_vendor_data = {
            'ad_account_id': job_scope.ad_account_id,
            'entity_type': job_scope.report_variant,
            'report_type': job_scope.report_type,
            ORGANIC_DATA_ENTITY_ID_MAP[job_scope.report_variant]: job_scope.entity_id,
        }

        data = list(data_iter)
        raw_record = {
            'payload': data,
            'page_id': job_scope.ad_account_id,
            ORGANIC_DATA_ENTITY_ID_MAP[job_scope.report_variant]: job_scope.entity_id,
        }
        vendor_data_raw = report_type_vendor_data_raw_extractor_map[job_scope.report_type](
            raw_record, **common_vendor_data
        )

        raw_record = add_vendor_data(raw_record, **vendor_data_raw)
        raw_store.store(raw_record)

        if len(data):
            # then, transpose it to correct form
            final_record = {
                'page_id': job_scope.ad_account_id,
                ORGANIC_DATA_ENTITY_ID_MAP[job_scope.report_variant]: job_scope.entity_id,
            }
            for param_datum in data:
                final_record[param_datum['name']] = param_datum['values'][0]['value']

            vendor_data = report_type_vendor_data_extractor_map[job_scope.report_type](raw_record, **common_vendor_data)
            final_record = add_vendor_data(final_record, **vendor_data)
            orig_store.store(final_record)

            yield final_record
