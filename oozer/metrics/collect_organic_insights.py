from typing import Dict, Any, Generator

from facebook_business.adobjects.insightsresult import InsightsResult

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import NAMESPACE_RAW
from common.tokens import PlatformTokenManager
from oozer.common.cold_storage import batch_store
from oozer.common.enum import ReportEntityApiKind, FB_AD_VIDEO_MODEL, ColdStoreBucketType
from oozer.common.facebook_api import PlatformApiContext
from oozer.common.job_scope import JobScope
from oozer.common.vendor_data import add_vendor_data

from oozer.metrics.constants import VIDEO_REPORT_METRICS, VIDEO_REPORT_FIELDS
from oozer.metrics.vendor_data_extractor import (
    report_type_vendor_data_extractor_map,
    report_type_vendor_data_raw_extractor_map,
    ORGANIC_DATA_PAGE_VIDEO_ID,
)


class OrganicJobScopeParsed:
    report_root_fb_entity = None

    def __init__(self, job_scope: JobScope):
        if job_scope.report_type not in [ReportType.lifetime]:
            raise ValueError(
                f"Report type {job_scope.report_type} specified is not one of supported values: "
                + ReportType.ALL_METRICS
            )
        # cool. we are in the right place...

        # direct, per-entity report
        entity_id = job_scope.entity_id
        entity_type = job_scope.entity_type
        entity_type_reporting = job_scope.report_variant

        with PlatformApiContext(job_scope.token) as fb_ctx:
            self.report_root_fb_entity = fb_ctx.to_fb_model(entity_id, entity_type)

        # here we configure code that will augment each datum with  record ID
        vendor_data_extractor = report_type_vendor_data_extractor_map[job_scope.report_type]

        aux_data = {
            'ad_account_id': job_scope.ad_account_id,
            'entity_type': entity_type_reporting,
            'report_type': job_scope.report_type,
        }

        self.augment_with_vendor_data = lambda data: add_vendor_data(data, **vendor_data_extractor(data, **aux_data))

    @staticmethod
    def detect_report_api_kind(job_scope: JobScope) -> str:
        if job_scope.entity_type == Entity.Page:
            return ReportEntityApiKind.Page
        elif job_scope.entity_type == Entity.PagePost:
            return ReportEntityApiKind.Post
        elif job_scope.entity_type == Entity.PageVideo:
            return ReportEntityApiKind.Video
        else:
            raise ValueError(f'Unknown entity type "{job_scope.entity_type}" when detecting report API kind')


class InsightsOrganic:
    @staticmethod
    def iter_video_insights(fb_entity: FB_AD_VIDEO_MODEL) -> Generator[Dict[str, Any], None, None]:
        """
        Run the actual execution of the insights job for video insights
        """

        params = {'metric': VIDEO_REPORT_METRICS}
        report_status_obj: InsightsResult = fb_entity.get_video_insights(params=params, fields=VIDEO_REPORT_FIELDS)

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
        # We don't use it for getting a token. Something else that calls us does.
        # However, we use it to report usages of the token we got.
        token_manager = PlatformTokenManager.from_job_scope(job_scope)

        report_entity_kind = OrganicJobScopeParsed.detect_report_api_kind(job_scope)
        scope_parsed = OrganicJobScopeParsed(job_scope)

        if report_entity_kind == ReportEntityApiKind.Video:
            data_iter = cls.iter_video_insights(scope_parsed.report_root_fb_entity)
            for datum in cls._iter_collect_video_insights(data_iter, job_scope):
                yield datum

                # right now, we support fetching video insights for only one video at a time
                # so no reason to report usage here
        else:
            raise ValueError(f'Unsupported report entity kind "{report_entity_kind}" to collect organic insights')

        token_manager.report_usage(token)

    @classmethod
    def _iter_collect_video_insights(
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
            ORGANIC_DATA_PAGE_VIDEO_ID: job_scope.entity_id,
        }

        data = list(data_iter)
        raw_record = {
            'payload': data,
            'page_id': job_scope.ad_account_id,
            ORGANIC_DATA_PAGE_VIDEO_ID: job_scope.entity_id,
        }
        vendor_data_raw = report_type_vendor_data_raw_extractor_map[job_scope.report_type](
            raw_record, **common_vendor_data
        )

        raw_record = add_vendor_data(raw_record, **vendor_data_raw)
        raw_store.store(raw_record)

        if len(data):
            # then, transpose it to correct form
            final_record = {'page_id': job_scope.ad_account_id, ORGANIC_DATA_PAGE_VIDEO_ID: job_scope.entity_id}
            for param_datum in data:
                final_record[param_datum['name']] = param_datum['values'][0]['value']

            vendor_data = report_type_vendor_data_extractor_map[job_scope.report_type](raw_record, **common_vendor_data)
            final_record = add_vendor_data(final_record, **vendor_data)
            orig_store.store(final_record)

            yield final_record
