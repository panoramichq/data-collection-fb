# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase
from datetime import datetime
import uuid

from facebookads.adobjects.campaign import Campaign
from oozer.cold_storage import ColdStorageUploader


class TestUploadToS3(TestCase):

    def test_uploads_successfully(self):
        test_campaign = Campaign('123123123')
        test_campaign[Campaign.Field.account_id] = '98989898'

        uploader = ColdStorageUploader(
            ad_account_id=test_campaign[Campaign.Field.account_id],
            report_type='fb_entities_adaccount_campaigns',
            report_time=datetime.now(),
            report_id=uuid.uuid4().hex,
            request_metadata={
                'some': 'metadata'
            }
        )

        uploader.write_items([
            test_campaign
        ])

        uploader.store()

