# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id
from tests.base import random

from oozer.common.sorted_jobs_queue import SortedJobsQueue, _JobsReader


class JobsWriterTests(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()

    def test_storage_works(self):

        # note the order of insertion - not in order of the score
        scored_jobs = [
            (
                generate_id(
                    ad_account_id='AAID',
                    report_type=ReportType.entity,
                    report_variant=Entity.Campaign
                ),
                20
            ),
            (
                generate_id(
                    ad_account_id='AAID',
                    report_type=ReportType.entity,
                    report_variant=Entity.AdSet
                ),
                30
            ),
            (
                generate_id(
                    ad_account_id='AAID',
                    report_type=ReportType.entity,
                    report_variant=Entity.Ad
                ),
                10
            ),
        ]

        extra_data = {
            'timezone': 'Europe/London'
        }

        with SortedJobsQueue(self.sweep_id).JobsWriter() as add_to_queue:
            for job_id, score in scored_jobs:
                # writes tasks to distributed sorting queues
                add_to_queue(job_id, score, **extra_data)

        jobs_queued_actual = []

        with SortedJobsQueue(self.sweep_id).JobsReader() as jobs_iter:
            for job_id, job_scope_data, score in jobs_iter:
                jobs_queued_actual.append(
                    (job_id, job_scope_data, score)
                )

        jobs_queued_should_be = [
            (
                generate_id(
                    ad_account_id='AAID',
                    report_type=ReportType.entity,
                    report_variant=Entity.AdSet
                ),
                {
                    'timezone': 'Europe/London'
                },
                30.0  # <-----
            ),
            (
                generate_id(
                    ad_account_id='AAID',
                    report_type=ReportType.entity,
                    report_variant=Entity.Campaign
                ),
                {
                    'timezone': 'Europe/London'
                },
                20.0  # <-----
            ),
            (
                generate_id(
                    ad_account_id='AAID',
                    report_type=ReportType.entity,
                    report_variant=Entity.Ad
                ),
                {
                    'timezone': 'Europe/London'
                },
                10.0  # <-----
            ),
        ]

        assert jobs_queued_actual == jobs_queued_should_be

    def test_sharding_does_not_break_and_does_not_duplicate(self):
        # Comes from understanding that we need to write more than some multiple
        # larger than number of internal shards (so currently 10, let's make it
        # double that for fun)
        jobs_to_generate = SortedJobsQueue._JOBS_READER_BATCH_SIZE * 20
        extra_data = {'timezone': 'Some/Thing'}

        jobs_to_write = [(
            generate_id(
                ad_account_id='AAID',
                entity_id=str(bogus_score),
                report_type=ReportType.entity,
                report_variant=Entity.Campaign
            ),
            bogus_score
        )
            for bogus_score in range(0, jobs_to_generate)
        ]

        with SortedJobsQueue(self.sweep_id).JobsWriter() as add_to_queue:
            for job_id, score in jobs_to_write:
                # writes tasks to distributed sorting queues
                add_to_queue(job_id, score, **extra_data)

        with SortedJobsQueue(self.sweep_id).JobsReader() as jobs_iter:
            cnt = 0

            for job_id, job_scope_data, score in jobs_iter:
                assert job_id is not None
                assert job_scope_data is not None
                assert score is not None
                cnt += 1

        assert cnt == jobs_to_generate


class TempTestingIterableUnpackingErrorCommunication(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()

    def test_len_able_object_of_wrong_type(self):
        import bugsnag

        redis = mock.Mock()
        # returns iterable of a tuple that has more than 2 elements (inner code expects 2)
        redis.zrevrange = mock.Mock(return_value=[[1,2,3]])

        with mock.patch.object(bugsnag, 'notify') as bug_notify:
            gg = _JobsReader._iter_tasks_per_key(
                'redis_key',
                redis,
                20
            )

            for _ in gg:
                # it's a generator
                # just need to spin it once to cause it to touch the code inside
                pass

        assert bug_notify.called

        aa, kk = bug_notify.call_args

        assert kk == {
            'meta_data': {
                'extra_data': {
                    'job_id_score_pair_type': str(list),
                    'job_id_score_pair_len': 3,
                    'job_id_score_pair_data': [1,2,3]
                }
            }
        }
