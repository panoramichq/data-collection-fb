from tests.base.testcase import TestCase
from oozer.metrics.collect_insights import FieldTransformation

# This data point is a mashup from two different ones (so that "offsite_conversion.fb_pixel_view_content" is present)
data = {
    "__api_version": "v3.1",
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
        "actions__link_click": "13637",
        "actions__link_click_1d_click": "13637",
        "actions__link_click_28d_click": "13637",
        "actions__post": "236",
        "actions__post_1d_click": "236",
        "actions__post_28d_click": "236",
        "actions__video_view": "208454",
        "actions__video_view_1d_click": "1917",
        "actions__video_view_1d_view": "206537",
        "actions__video_view_28d_click": "1917",
        "actions__video_view_28d_view": "206537",
        "actions__comment": "110",
        "actions__comment_1d_click": "110",
        "actions__comment_28d_click": "110",
        "actions__post_reaction": "1490",
        "actions__post_reaction_1d_click": "1490",
        "actions__post_reaction_28d_click": "1490",
        "actions__page_engagement": "223927",
        "actions__page_engagement_1d_click": "17390",
        "actions__page_engagement_1d_view": "206537",
        "actions__page_engagement_28d_view": "206537",
        "actions__page_engagement_28d_click": "17390",
        "actions__post_engagement": "223927",
        "actions__post_engagement_1d_click": "17390",
        "actions__post_engagement_1d_view": "206537",
        "actions__post_engagement_28d_click": "17390",
        "actions__post_engagement_28d_view": "206537",

        "unique_actions__link_click": "5",
        "unique_actions__link_click_1d_click": "5",
        "unique_actions__link_click_28d_click": "5",

        "unique_actions__offsite_conversion_fb_pixel_view_content": "1",
        "unique_actions__offsite_conversion_fb_pixel_view_content_1d_click": "1",
        "unique_actions__offsite_conversion_fb_pixel_view_content_28d_click": "1",

        "unique_actions__post_engagement": "5",
        "unique_actions__post_engagement_1d_click": "5",
        "unique_actions__post_engagement_28d_click": "5",

        "unique_actions__page_engagement": "5",
        "unique_actions__page_engagement_1d_click": "5",
        "unique_actions__page_engagement_28d_click": "5",

        "unique_actions__offsite_conversion": "1",
        "unique_actions__offsite_conversion_1d_click": "1",
        "unique_actions__offsite_conversion_28d_click": "1",
    },
}


class InsightTransformationTest(TestCase):
    def test_basic_transformation(self):
        self.maxDiff = None
        with_transformed = FieldTransformation.transform(data, ['actions', 'unique_actions'])
        self.assertDictEqual(with_transformed, expected)

    def test_on_a_datum_without_action_field(self):
        with_transformed = FieldTransformation.transform(data, ['some_very_special_field'])

        # In that case the input should be unchanged
        self.assertDictEqual(with_transformed, data)

    def test_emtpy_datum_transformation(self):
        with_transformed = FieldTransformation.transform({}, ['some_very_special_field'])
        self.assertDictEqual(with_transformed, {})
