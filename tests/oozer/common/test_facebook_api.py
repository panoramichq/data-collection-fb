# this patches gevent and other things properly. Don't move down.
# Must be first import for all test files
from tests.base.testcase import TestCase


from unittest.mock import Mock, patch

from oozer.common.facebook_api import (
    transform_buc_data_to_generic_local,
    FacebookAdsApi,
    OriginalFacebookAdsApi
)


class TestFacebookApiOverridesThrottlingHeaderTX(TestCase):

    def test_buc_data_transform_1(self):

        multi_data = {
            "1908934339393402": [
                {
                    "call_count": 1,
                    "estimated_time_to_regain_access": 0,
                    "total_cputime": 1,
                    "total_time": 1,
                    "type": "ads_management"
                }
            ],
            "1908934339393403": [
                {
                    "call_count": 1,
                    "estimated_time_to_regain_access": 0,
                    "total_cputime": 1,
                    "total_time": 1,
                    "type": "ads_management"
                }
            ]
        }

        should_be = {
            "ads_management": {
                "call_count": 1,
                "estimated_time_to_regain_access": 0,
                "total_cputime": 1,
                "total_time": 1,
            }
        }

        assert transform_buc_data_to_generic_local(multi_data) == should_be

    def test_buc_data_transform_2(self):

        multi_data = {
            "1908934339393402": [
                {
                    "call_count": 1,
                    "estimated_time_to_regain_access": 0,
                    "total_cputime": 1,
                    "total_time": 1,
                    "type": "ads_management"
                },
                {
                    "call_count": 1,
                    "estimated_time_to_regain_access": 0,
                    "total_cputime": 1,
                    "total_time": 1,
                    "type": "ads_insights"
                }
            ],
            "1908934339393403": [
                {
                    "call_count": 1,
                    "estimated_time_to_regain_access": 0,
                    "total_cputime": 1,
                    "total_time": 1,
                    "type": "ads_management"
                }
            ]
        }

        should_be = {
            "ads_management": {
                "call_count": 1,
                "estimated_time_to_regain_access": 0,
                "total_cputime": 1,
                "total_time": 1,
            },
            "ads_insights": {
                "call_count": 1,
                "estimated_time_to_regain_access": 0,
                "total_cputime": 1,
                "total_time": 1,
            }
        }

        assert transform_buc_data_to_generic_local(multi_data) == should_be

    def test_buc_data_transform_bad_data_1(self):
        multi_data = 'yep'
        should_be = {}
        assert transform_buc_data_to_generic_local(multi_data) == should_be

    def test_buc_data_transform_bad_data_2(self):
        multi_data = {'yep':1}
        should_be = {}
        assert transform_buc_data_to_generic_local(multi_data) == should_be

    def test_buc_data_transform_bad_data_3(self):
        multi_data = {
            "1908934339393402": 'asdf'
        }
        should_be = {}
        assert transform_buc_data_to_generic_local(multi_data) == should_be

    def test_buc_data_transform_bad_data_4(self):
        multi_data = {
            "1908934339393402": [
                'asdf',
                'zxcv'
            ],
        }
        should_be = {}
        assert transform_buc_data_to_generic_local(multi_data) == should_be

    def test_buc_data_transform_bad_data_5(self):
        multi_data = {
            "1908934339393402": [
                {
                    "call_count": 1,
                    "estimated_time_to_regain_access": 0,
                    "total_cputime": 1,
                    "total_time": 1,
                    "type-BROKEN_HERE": "ads_management"
                },
                {
                    "call_count": 1,
                    "estimated_time_to_regain_access": 0,
                    "total_cputime": 1,
                    "total_time": 1,
                    "type": "ads_insights"
                }
            ],
        }
        should_be = {
            'ads_insights': {
                "call_count": 1,
                "estimated_time_to_regain_access": 0,
                "total_cputime": 1,
                "total_time": 1,
            }
        }

        assert transform_buc_data_to_generic_local(multi_data) == should_be


class TestFacebookApiThrottlingPercentage(TestCase):

    def test_percentage_multi_good(self):

        throttling_metrics = {
            "ads_management": {
                "call_count": 1,
                "estimated_time_to_regain_access": 0,
                "total_cputime": 2,
                "total_time": 3,
            },
            "ads_insights": {
                "call_count": 4,
                "estimated_time_to_regain_access": 0,
                "total_cputime": 5,
                "total_time": 6,
            }
        }

        api = FacebookAdsApi(None)
        api.throttling_metrics = throttling_metrics

        assert api.throttling_percentage == 6

    def test_percentage_multi_half_good(self):

        throttling_metrics = {
            "ads_management": 'asdf',
            "ads_insights": {
                "call_count": 4,
                "estimated_time_to_regain_access": 0,
                "total_cputime": 5,
                "total_time": 6,
            }
        }

        api = FacebookAdsApi(None)
        api.throttling_metrics = throttling_metrics

        assert api.throttling_percentage == 6

    def test_percentage_multi_bad(self):

        throttling_metrics = {
            "ads_management": 'asdf'
        }

        api = FacebookAdsApi(None)
        api.throttling_metrics = throttling_metrics

        assert api.throttling_percentage == 0


class TestFacebookApiThrottlingWait(TestCase):

    def test_percentage_multi_good(self):

        throttling_metrics = {
            "ads_management": {
                "call_count": 1,
                "estimated_time_to_regain_access": 20,
                "total_cputime": 2,
                "total_time": 3,
            },
            "ads_insights": {
                "call_count": 4,
                "estimated_time_to_regain_access": 10,
                "total_cputime": 5,
                "total_time": 6,
            }
        }

        api = FacebookAdsApi(None)
        api.throttling_metrics = throttling_metrics

        assert api.throttling_wait == 20 * 60

    def test_percentage_multi_half_good(self):

        throttling_metrics = {
            "ads_management": 'asdf',
            "ads_insights": {
                "call_count": 4,
                "estimated_time_to_regain_access": 10,
                "total_cputime": 5,
                "total_time": 6,
            }
        }

        api = FacebookAdsApi(None)
        api.throttling_metrics = throttling_metrics

        assert api.throttling_wait == 10 * 60

    def test_percentage_multi_bad(self):

        throttling_metrics = {
            "ads_management": 'asdf'
        }

        api = FacebookAdsApi(None)
        api.throttling_metrics = throttling_metrics

        assert api.throttling_wait == 0

