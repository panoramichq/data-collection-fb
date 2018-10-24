from facebook_business.adobjects.adaccount import AdAccount

from tests.base.testcase import TestCase, integration

from datetime import datetime

from oozer.common.job_scope import JobScope
from oozer.entities.collect_adaccount import collect_adaccount

from common.enums.entity import Entity
from config.facebook import TOKEN, AD_ACCOUNT
from oozer.common.facebook_api import get_default_fields


@integration('facebook')
class TestingAdAccountCollectionPipeline(TestCase):

    def test_fetch_ad_account_pipeline(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            entity_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.AdAccount,
            sweep_id='1'
        )

        aa_data = collect_adaccount(job_scope, None)
        aa_data_keys = aa_data.keys()

        assert aa_data

        for required_field in  get_default_fields(AdAccount):
            assert required_field in aa_data_keys, f'{required_field} should be in the response from FB'
