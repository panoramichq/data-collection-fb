# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

import time

from config import test as test_config
from oozer.tasks import fb_entities_adaccount_campaigns


class TestFacebookEntitiesFetch(TestCase):

    def setUp(self):
        self.job_context = {
            'sweep_id': time.time(),
            'report_type': 'e'
        }

    def test_fetch_campaigns(self):

        # Do some sort of patching or strategy selection for shipping the
        # collected data so we can do asserts on it

        # You need to run this with:
        # APP_CELERY_task_always_eager pytest tests/whatever
        context = {}

        fb_entities_adaccount_campaigns.delay(
            test_config.FB_ACCESS_TOKEN, test_config.FB_ADACCOUNT_ID,
            self.job_context, context
        ).get()

    def test_fetch_adsets(self):
        pass

    def test_fetch_ads(self):
        pass
