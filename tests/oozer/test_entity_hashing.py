# must be first, as it does event loop patching and other "first" things

from tests.base.testcase import TestCase
from unittest import skip

from twitter_ads.campaign import Campaign

from datetime import datetime
import pytz

from oozer.common import job_scope
from oozer.entities.entity_hash import EntityHash, checksum_entity

from common.facebook.enums.entity import Entity
from config.facebook import AD_ACCOUNT, TOKEN


# FIXME: Change the test for twitter
@skip
class TestEntityHasher(TestCase):

    def _manufacture_job_scope(self):
        return job_scope.JobScope(
            ad_account_id=AD_ACCOUNT,
            report_type='entity',
            report_time=datetime.now(pytz.utc),
            report_id="some_id",
            sweep_id='12',
            report_variant=Entity.Campaign,
        )

    def _manufacture_test_entity(self):
        c = Campaign('3842799601730217')
        # TODO: Add fields to the test campaign object
        return c

    def test_hash_calculation(self):
        checksum = checksum_entity(
            self._manufacture_test_entity()
        )

        assert checksum == EntityHash(
            data='e92f1414ef60429f',
            fields='ae4bf294cdfa2f3a'
        )

    def test_field_selection(self):
        entity = self._manufacture_test_entity()
        checksum = checksum_entity(
            entity, ['account_id', 'buying_type']
        )

        assert checksum == EntityHash(
            data='298161fe360af3f3',
            fields='1558fc6663140a4e'
        )

        # Check changing of value not selected does not break anything
        entity['objective'] = 'POST_ENGAGEMENT'
        checksum = checksum_entity(
            entity, ['account_id', 'buying_type']
        )

        assert checksum == EntityHash(
            data='298161fe360af3f3',
            fields='1558fc6663140a4e'
        )
