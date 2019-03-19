# must be first, as it does event loop patching and other "first" things
from oozer.common.enum import ColdStoreBucketType
from tests.base.testcase import TestCase, mock, skip

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from oozer.common.job_scope import JobScope
from tests.base.random import gen_string_id

from oozer.common.cold_storage.batch_store import ChunkDumpStore, NormalStore
from oozer.common.cold_storage.base_store import load_data


class TestBatchStore(TestCase):
    def setUp(self):

        self.job_scope = JobScope(
            sweep_id=gen_string_id(),
            ad_account_id=gen_string_id(),
            entity_id=gen_string_id(),
            entity_type=Entity.Campaign,
            report_type=ReportType.entity,
        )

    @skip
    def test_normal_store_works(self):

        # Here we shoot all teh way to S3 for one reason:
        # to ensure that our way of wrapping base `store`
        # as method on an instance of context manager
        # This test is here to ensure that all other tests in this file,
        # though mocking out the call to underlying `store` would still
        # work as expected, as all variants of context-manager-based Store
        # implementations call underlying `store` in exactly same way.

        data = {'a': 'b'}

        with NormalStore(self.job_scope) as store:
            key = store(data)

        assert load_data(key) == [data]  # data auto-packaged into array in store()

    def test_chunked_dump_store(self):

        data_iter = [{'id': 1}, {'id': 2}, {'id': 3}]

        with mock.patch.object(ChunkDumpStore, '_store') as _store:

            with ChunkDumpStore(self.job_scope, chunk_size=2) as store:
                for datum in data_iter:
                    store(datum)  # copy is to separate value from `datum` variable

        assert len(_store.call_args_list) == 2
        sig1, sig2 = _store.call_args_list

        aa, kk = sig1
        assert not kk
        assert aa == ([{'id': 1}, {'id': 2}], self.job_scope, 0, ColdStoreBucketType.ORIGINAL_BUCKET, None)  # chunk ID

        aa, kk = sig2
        assert not kk
        assert aa == ([{'id': 3}], self.job_scope, 1, ColdStoreBucketType.ORIGINAL_BUCKET, None)  # chunk ID
