# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase
from datetime import datetime
import pytz
from facebookads.adobjects.campaign import Campaign

from oozer.common import job_scope
from common.enums.entity import Entity

from config.facebook import AD_ACCOUNT, TOKEN

from oozer.entities.collect_entities_per_adaccount import EntityHash, \
    _checksum_entity


class TestEntityHasher(TestCase):

    def _manufacture_job_scope(self):
        return job_scope.JobScope(
            ad_account_id=AD_ACCOUNT,
            report_type='entity',
            report_time=datetime.now(pytz.utc),
            report_id="some_id",
            sweep_id='12',
            report_variant=Entity.Campaign,
            tokens=[TOKEN]
        )

    def _manufacture_test_entity(self):
        c = Campaign('3842799601730217')
        c['account_id'] = '2049751338645034'
        c['budget_rebalance_flag'] = False
        c['buying_type'] = 'RESERVED'
        c['configured_status'] = 'ACTIVE'
        return c

    def test_hash_calculation(self):
        checksum = _checksum_entity(
            self._manufacture_test_entity()
        )

        assert checksum == EntityHash(
            data='f9226b08eaa500a1',
            fields='4be66693c426f152'
        )

    def test_field_selection(self):
        entity = self._manufacture_test_entity()
        checksum = _checksum_entity(
            entity, ['account_id', 'buying_type']
        )

        assert checksum == EntityHash(
            data='298161fe360af3f3',
            fields='1558fc6663140a4e'
        )

        # Check changing of value not selected does not break anything
        entity['budget_rebalance_flag'] = 'whatever'
        checksum = _checksum_entity(
            entity, ['account_id', 'buying_type']
        )

        assert checksum == EntityHash(
            data='298161fe360af3f3',
            fields='1558fc6663140a4e'
        )
