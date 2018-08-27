from tests.base.testcase import TestCase, mock
from tests.base import random

from datetime import datetime

from oozer.common.enum import JobStatus
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from common.enums.entity import Entity
from oozer.entities.collect_adaccount import collect_adaccount


class TestCollectAdAccount(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_fails_with_wrong_report_variant(self):
        job_scope = JobScope(
            ad_account_id=self.ad_account_id,
            tokens=['blah'],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=None, # This actually should be set to AdAccount
            sweep_id='1'
        )

        with mock.patch.object(report_job_status_task, 'delay') as status_task, \
            self.assertRaises(ValueError) as ex_trap:
            collect_adaccount(job_scope, None)

        assert 'Report level' in str(ex_trap.exception)
        assert status_task.called
        parameters, _ = status_task.call_args
        assert (JobStatus.GenericError, job_scope) == parameters, 'Must report status correctly on failure'

    def test_fails_without_a_token(self):
        job_scope = JobScope(
            ad_account_id=self.ad_account_id,
            tokens=[None],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.AdAccount, # This actually should be set to AdAccount
            sweep_id='1'
        )

        with mock.patch.object(report_job_status_task, 'delay') as status_task, \
            self.assertRaises(ValueError) as ex_trap:
            collect_adaccount(job_scope, None)

        assert 'token' in str(ex_trap.exception)
        assert status_task.called
        parameters, _ = status_task.call_args
        assert (JobStatus.GenericError, job_scope) == parameters, 'Must report status correctly on failure'