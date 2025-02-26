import logging

from facebook_business.adobjects.adsinsights import AdsInsights
from typing import Dict, List, Union, Optional, Callable

from common.enums.entity import Entity
from oozer.common.job_scope import JobScope
from oozer.common.enum import JobStatus, ColdStoreBucketType

from oozer.common.cold_storage.base_store import store, DEFAULT_CHUNK_NUMBER

logger = logging.getLogger(__name__)


class BaseStoreHandler:

    _store: Callable[[Union[List, Dict], JobScope, Optional[int]], None] = staticmethod(store)

    def __init__(self, job_scope: JobScope):
        self.job_scope = job_scope

    def store(self, datum):
        # self._store(datum, extra stuff)
        pass

    def __enter__(self):
        return self.store

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class NormalStore(BaseStoreHandler):
    def __init__(self, job_scope, bucket_type: str = ColdStoreBucketType.ORIGINAL_BUCKET, custom_namespace: str = None):
        super().__init__(job_scope)
        self.data = []
        self.bucket_type = bucket_type
        self.custom_namespace = custom_namespace

    def store(self, datum):
        return self._store(datum, self.job_scope, DEFAULT_CHUNK_NUMBER, self.bucket_type, self.custom_namespace)


class MemorySpoolStore(BaseStoreHandler):
    def __init__(self, job_scope: JobScope):
        super().__init__(job_scope)
        self.data = []

    def store(self, datum):
        self.data.append(datum)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._store(self.data, self.job_scope)


class ChunkDumpStore(BaseStoreHandler):
    def __init__(
        self,
        job_scope,
        chunk_size=50,
        bucket_type: str = ColdStoreBucketType.ORIGINAL_BUCKET,
        custom_namespace: str = None,
    ):
        super().__init__(job_scope)
        self.data = []
        self.chunk_marker = 0
        self.chunk_size = chunk_size
        self.bucket_type = bucket_type
        self.custom_namespace = custom_namespace

    def store(self, datum):
        self.data.append(datum)
        if len(self.data) == self.chunk_size:
            self._store(self.data, self.job_scope, self.chunk_marker, self.bucket_type, self.custom_namespace)
            self.chunk_marker += 1
            self.data = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        if len(self.data):
            self._store(self.data, self.job_scope, self.chunk_marker, self.bucket_type, self.custom_namespace)


class NaturallyNormativeChildStore(BaseStoreHandler):
    def __init__(self, job_scope: JobScope, bucket_type: str = ColdStoreBucketType.ORIGINAL_BUCKET):
        super().__init__(job_scope)

        normative_entity_type = job_scope.report_variant
        assert normative_entity_type in Entity.ALL

        self.job_scope_base_data = job_scope.to_dict()
        # since we are converting per-parent into per-child
        # job signature, report_variant cannot be set
        self.job_scope_base_data.update(
            entity_type=normative_entity_type,
            is_derivative=True,  # this keeps the scope from being counted as done task by looper
            report_variant=None,
        )
        self.id_attribute_name = {
            Entity.AdAccount: AdsInsights.Field.account_id,
            Entity.Campaign: AdsInsights.Field.campaign_id,
            Entity.AdSet: AdsInsights.Field.adset_id,
            Entity.Ad: AdsInsights.Field.ad_id,
        }[normative_entity_type]
        self.bucket_type = bucket_type

    def store(self, datum):
        from oozer.common.report_job_status_task import report_job_status_task

        entity_id = datum.get(self.id_attribute_name) or datum.get('id')
        assert entity_id, "This code must have an entity ID for building of unique insertion ID"
        normative_job_scope = JobScope(self.job_scope_base_data, entity_id=entity_id)
        # and store data under that per-entity, normative JobScope.
        self._store(datum, normative_job_scope, DEFAULT_CHUNK_NUMBER, self.bucket_type)
        # since we report for many entities in this code,
        # must also communicate out the status inside of the for-loop
        # at the normative level.
        report_job_status_task.delay(JobStatus.Done, normative_job_scope)
