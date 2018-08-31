from facebook_business.adobjects.adaccount import AdAccount

from tests.base.testcase import TestCase, integration

from datetime import datetime
from unittest import skip, mock

from oozer.common.job_scope import JobScope
from oozer.entities.collect_adaccount import collect_adaccount

from common.enums.entity import Entity
from config.facebook import TOKEN, AD_ACCOUNT
from oozer.common.facebook_api import PlatformApiContext, get_default_fields


# from oozer.entities.collect_entities_per_adaccount import \
#     , iter_native_entities_per_adaccount


@integration('facebook')
class TestingAdAccountCollectionPipeline(TestCase):

    def test_fetch_ad_account_pipeline(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.AdAccount,
            sweep_id='1'
        )

        aa_data = collect_adaccount(job_scope, None)
        aa_data_keys = aa_data.keys()

        assert aa_data

        all_required_fields = filter(lambda field: field != 'rate_limit_reset_time', get_default_fields(AdAccount)) # FIXME: seems rate_limit_reset_time gets returned never or  not all the time

        for required_field in all_required_fields:
            assert required_field in aa_data_keys, f'{required_field} should be in the response from FB'
