# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from facebookads.adobjects import campaign
from config import test as test_config

from oozer.tasks import feedback_entity


class TestEntityFeedback(TestCase):

    def _test_campaign_factory(self):
        return campaign.Campaign(123123, test_config.FB_ADACCOUNT_ID)

    def test_campaign_reported(self):

        mock_campaign = self._test_campaign_factory()

        feedback_entity.delay('facebook', mock_campaign).get()




