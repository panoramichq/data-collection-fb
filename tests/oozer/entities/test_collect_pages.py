from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from datetime import datetime
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.page import Page
from facebook_business.api import FacebookRequest

from oozer.common.sweep_running_flag import SweepRunningFlag
from tests.base import random
from tests.base.testcase import TestCase, mock
from oozer.common.enum import JobStatus
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.entities.collect_pages import collect_pages_from_business_task


class TestCollectPages(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()

    def test_fails_with_wrong_report_variant(self):
        job_scope = JobScope(
            tokens=['blah'], report_time=datetime.utcnow(), report_type='entity', report_variant=None, sweep_id='1'
        )

        with SweepRunningFlag(job_scope.sweep_id), \
            mock.patch.object(report_job_status_task, 'delay') as status_task, \
                self.assertRaises(ValueError) as ex_trap:
            collect_pages_from_business_task(job_scope, None)

            assert 'Report level' in str(ex_trap.exception)
            assert status_task.called
            parameters, _ = status_task.call_args
            assert (JobStatus.GenericError, job_scope) == parameters, 'Must report status correctly on failure'

    def test_fails_without_a_token(self):
        job_scope = JobScope(
            tokens=[None],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.Page,
            sweep_id='1'
        )

        with SweepRunningFlag(job_scope.sweep_id), \
            mock.patch.object(report_job_status_task, 'delay') as status_task, \
                self.assertRaises(ValueError) as ex_trap:
            collect_pages_from_business_task(job_scope, None)

            assert 'token' in str(ex_trap.exception)
            assert status_task.called

            status_task_args, _ = status_task.call_args
            assert (JobStatus.GenericError, job_scope) == status_task_args, 'Must report status correctly on failure'

    def test_runs_correctly(self):
        biz_id_1 = random.gen_string_id()
        biz_id_2 = random.gen_string_id()
        page_id_1 = random.gen_string_id()
        page_id_2 = random.gen_string_id()
        page_id_3 = random.gen_string_id()
        page_id_4 = random.gen_string_id()

        businesses = [Business(fbid=biz_id_1), Business(fbid=biz_id_2)]

        client_pages = [Page(fbid=page_id_1), Page(fbid=page_id_2)]

        owned_pages = [Page(fbid=page_id_3), Page(fbid=page_id_4)]

        job_scope = JobScope(
            ad_account_id='0',
            sweep_id=self.sweep_id,
            entity_type=Entity.Scope,
            entity_id=self.scope_id,
            report_type=ReportType.import_pages,
            report_variant=Entity.Page,
            tokens=['token']
        )

        with SweepRunningFlag(job_scope.sweep_id), \
            mock.patch.object(FacebookRequest, 'execute', return_value=businesses) as gp, \
            mock.patch.object(Business, 'get_client_pages', return_value=client_pages), \
            mock.patch.object(Business, 'get_owned_pages', return_value=owned_pages), \
                mock.patch.object(report_job_status_task, 'delay') as status_task:
            collect_pages_from_business_task(job_scope, None)

            assert gp.called

            assert status_task.called
            # it was called many times, but we care about the last time only
            pp, kk = status_task.call_args
            assert not kk
            assert pp == (JobStatus.Done, job_scope)
