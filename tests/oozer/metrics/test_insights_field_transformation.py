from tests.base.testcase import TestCase
from oozer.metrics.field_transformation import FieldTransformation

# This data point is a mashup from two different ones (so that "offsite_conversion.fb_pixel_view_content" is present)
data = {
    "__api_version": "v3.3",
    "__extracted_at": "2019-03-23 05:44:55.378191",
    "__oprm": {
        "entity_id": "23843314131640341",
        "entity_type": "AS",
        "id": "oprm|m|fb|2235041426568730|AS|23843314131640341|lifetime",
    },
    "__processed_at": "2019-03-23 05:44:56.083555",
    "__record_id": "oprm|m|fb|2235041426568730|AS|23843314131640341|lifetime",
    "account_id": "2235041426568730",
    "actions": [
        {"1d_click": "13637", "28d_click": "13637", "action_type": "link_click", "value": "13637"},
        {"1d_click": "236", "28d_click": "236", "action_type": "post", "value": "236"},
        {
            "1d_click": "1917",
            "1d_view": "206537",
            "28d_click": "1917",
            "28d_view": "206537",
            "action_type": "video_view",
            "value": "208454",
        },
        {"1d_click": "110", "28d_click": "110", "action_type": "comment", "value": "110"},
        {"1d_click": "1490", "28d_click": "1490", "action_type": "post_reaction", "value": "1490"},
        {
            "1d_click": "17390",
            "1d_view": "206537",
            "28d_click": "17390",
            "28d_view": "206537",
            "action_type": "page_engagement",
            "value": "223927",
        },
        {
            "1d_click": "17390",
            "1d_view": "206537",
            "28d_click": "17390",
            "28d_view": "206537",
            "action_type": "post_engagement",
            "value": "223927",
        },
    ],
    "adset_id": "23843314131640341",
    "campaign_id": "23843314130600341",
    "clicks": "20305",
    "cpc": "0.169779",
    "cpm": "8.077264",
    "cpp": "13.3",
    "ctr": "4.75752",
    "date_start": "2018-10-19",
    "date_stop": "2019-03-22",
    "frequency": "1.646597",
    "impressions": "426798",
    "reach": "259200",
    "spend": "3447.36",
    "unique_actions": [  # Unique actions are taken from a different datapoint - that's why they don't match actions
        {"1d_click": "5", "28d_click": "5", "action_type": "link_click", "value": "5"},
        {"1d_click": "1", "28d_click": "1", "action_type": "offsite_conversion.fb_pixel_view_content", "value": "1"},
        {"1d_click": "5", "28d_click": "5", "action_type": "post_engagement", "value": "5"},
        {"1d_click": "5", "28d_click": "5", "action_type": "page_engagement", "value": "5"},
        {"1d_click": "1", "28d_click": "1", "action_type": "offsite_conversion", "value": "1"},
    ],
}

expected = {
    **data,
    '__transformed': {
        "actions": {
            "link_click": {"value": "13637", "1d_click": "13637", "28d_click": "13637"},
            "post": {"value": "236", "1d_click": "236", "28d_click": "236"},
            "video_view": {
                "value": "208454",
                "1d_click": "1917",
                "1d_view": "206537",
                "28d_click": "1917",
                "28d_view": "206537",
            },
            "comment": {"value": "110", "1d_click": "110", "28d_click": "110"},
            "post_reaction": {"value": "1490", "1d_click": "1490", "28d_click": "1490"},
            "page_engagement": {
                "value": "223927",
                "1d_click": "17390",
                "1d_view": "206537",
                "28d_view": "206537",
                "28d_click": "17390",
            },
            "post_engagement": {
                "value": "223927",
                "1d_click": "17390",
                "1d_view": "206537",
                "28d_view": "206537",
                "28d_click": "17390",
            },
        },
        "unique_actions": {
            "link_click": {"value": "5", "1d_click": "5", "28d_click": "5"},
            "offsite_conversion.fb_pixel_view_content": {"value": "1", "1d_click": "1", "28d_click": "1"},
            "post_engagement": {"value": "5", "1d_click": "5", "28d_click": "5"},
            "page_engagement": {"value": "5", "1d_click": "5", "28d_click": "5"},
            "offsite_conversion": {"value": "1", "1d_click": "1", "28d_click": "1"},
        },
    },
}


class InsightTransformationTest(TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_basic_transformation(self):
        with_transformed = FieldTransformation.transform(data, ['actions', 'unique_actions'])
        self.assertDictEqual(with_transformed, expected)

    def test_on_a_datum_without_action_field(self):
        with_transformed = FieldTransformation.transform(data, ['some_very_special_field'])

        # In that case the input should contain a blank __transformed dict
        self.assertDictEqual(with_transformed, {**data, '__transformed': {}})

    def test_emtpy_datum_transformation(self):
        with_transformed = FieldTransformation.transform({}, ['some_very_special_field'])
        # In that case the input should contain a blank __transformed dict
        self.assertDictEqual(with_transformed, {'__transformed': {}})
