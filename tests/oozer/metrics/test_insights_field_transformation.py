from tests.base.testcase import TestCase
from oozer.metrics.collect_insights import FieldTransformation

data = {
    "__api_version": "v3.1",
    "__extracted_at": "2019-03-23 05:44:55.378191",
    "__oprm": {
        "entity_id": "23843314131640341",
        "entity_type": "AS",
        "id": "oprm|m|fb|2235041426568730|AS|23843314131640341|lifetime"
    },
    "__processed_at": "2019-03-23 05:44:56.083555",
    "__record_id": "oprm|m|fb|2235041426568730|AS|23843314131640341|lifetime",
    "account_id": "2235041426568730",
    "actions": [
        {
            "1d_click": "13637",
            "28d_click": "13637",
            "action_type": "link_click",
            "value": "13637"
        },
        {
            "1d_click": "236",
            "28d_click": "236",
            "action_type": "post",
            "value": "236"
        },
        {
            "1d_click": "1917",
            "1d_view": "206537",
            "28d_click": "1917",
            "28d_view": "206537",
            "action_type": "video_view",
            "value": "208454"
        },
        {
            "1d_click": "110",
            "28d_click": "110",
            "action_type": "comment",
            "value": "110"
        },
        {
            "1d_click": "1490",
            "28d_click": "1490",
            "action_type": "post_reaction",
            "value": "1490"
        },
        {
            "1d_click": "17390",
            "1d_view": "206537",
            "28d_click": "17390",
            "28d_view": "206537",
            "action_type": "page_engagement",
            "value": "223927"
        },
        {
            "1d_click": "17390",
            "1d_view": "206537",
            "28d_click": "17390",
            "28d_view": "206537",
            "action_type": "post_engagement",
            "value": "223927"
        }
    ],
    "adset_id": "23843314131640341",
    "campaign_id": "23843314130600341",
    "clicks": "20305",
    "cost_per_action_type": [
        {
            "1d_click": "0.252795",
            "28d_click": "0.252795",
            "action_type": "link_click",
            "value": "0.252795"
        },
        {
            "1d_click": "14.607458",
            "28d_click": "14.607458",
            "action_type": "post",
            "value": "14.607458"
        },
        {
            "1d_click": "1.79831",
            "1d_view": "0.016691",
            "28d_click": "1.79831",
            "28d_view": "0.016691",
            "action_type": "video_view",
            "value": "0.016538"
        },
        {
            "1d_click": "31.339636",
            "28d_click": "31.339636",
            "action_type": "comment",
            "value": "31.339636"
        },
        {
            "1d_click": "2.313664",
            "28d_click": "2.313664",
            "action_type": "post_reaction",
            "value": "2.313664"
        },
        {
            "1d_click": "0.198238",
            "1d_view": "0.016691",
            "28d_click": "0.198238",
            "28d_view": "0.016691",
            "action_type": "page_engagement",
            "value": "0.015395"
        },
        {
            "1d_click": "0.198238",
            "1d_view": "0.016691",
            "28d_click": "0.198238",
            "28d_view": "0.016691",
            "action_type": "post_engagement",
            "value": "0.015395"
        }
    ],
    "cost_per_unique_action_type": [
        {
            "1d_click": "0.279365",
            "28d_click": "0.279365",
            "action_type": "link_click",
            "value": "0.279365"
        },
        {
            "1d_click": "15.186608",
            "28d_click": "15.186608",
            "action_type": "post",
            "value": "15.186608"
        },
        {
            "1d_click": "1.539",
            "1d_view": "0.022786",
            "28d_click": "1.539",
            "28d_view": "0.022786",
            "action_type": "video_view",
            "value": "0.022519"
        },
        {
            "1d_click": "31.627156",
            "28d_click": "31.627156",
            "action_type": "comment",
            "value": "31.627156"
        },
        {
            "1d_click": "2.485479",
            "28d_click": "2.485479",
            "action_type": "post_reaction",
            "value": "2.485479"
        },
        {
            "1d_click": "0.225377",
            "1d_view": "0.022786",
            "28d_click": "0.225377",
            "28d_view": "0.022786",
            "action_type": "page_engagement",
            "value": "0.022453"
        },
        {
            "1d_click": "0.225377",
            "1d_view": "0.022786",
            "28d_click": "0.225377",
            "28d_view": "0.022786",
            "action_type": "post_engagement",
            "value": "0.022453"
        }
    ],
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
    "unique_actions": [
        {
            "1d_click": "12340",
            "28d_click": "12340",
            "action_type": "link_click",
            "value": "12340"
        },
        {
            "1d_click": "227",
            "28d_click": "227",
            "action_type": "post",
            "value": "227"
        },
        {
            "1d_click": "2240",
            "1d_view": "151296",
            "28d_click": "2240",
            "28d_view": "151296",
            "action_type": "video_view",
            "value": "153088"
        },
        {
            "1d_click": "109",
            "28d_click": "109",
            "action_type": "comment",
            "value": "109"
        },
        {
            "1d_click": "1387",
            "28d_click": "1387",
            "action_type": "post_reaction",
            "value": "1387"
        },
        {
            "1d_click": "15296",
            "1d_view": "151296",
            "28d_click": "15296",
            "28d_view": "151296",
            "action_type": "page_engagement",
            "value": "153536"
        },
        {
            "1d_click": "15296",
            "1d_view": "151296",
            "28d_click": "15296",
            "28d_view": "151296",
            "action_type": "post_engagement",
            "value": "153536"
        }
    ],
    "video_10_sec_watched_actions": [
        {
            "1d_click": "1625",
            "1d_view": "104162",
            "28d_click": "1625",
            "28d_view": "104162",
            "action_type": "video_view",
            "value": "105787"
        }
    ],
    "video_30_sec_watched_actions": [
        {
            "1d_click": "1410",
            "1d_view": "65332",
            "28d_click": "1410",
            "28d_view": "65332",
            "action_type": "video_view",
            "value": "66742"
        }
    ],
    "video_avg_percent_watched_actions": [
        {
            "1d_click": "108.37",
            "1d_view": "49.93",
            "28d_click": "108.37",
            "28d_view": "49.93",
            "action_type": "video_view",
            "value": "50.27"
        }
    ],
    "video_avg_time_watched_actions": [
        {
            "1d_click": "24",
            "1d_view": "10",
            "28d_click": "24",
            "28d_view": "10",
            "action_type": "video_view",
            "value": "10"
        }
    ],
    "video_p100_watched_actions": [
        {
            "1d_click": "1379",
            "1d_view": "64319",
            "28d_click": "1379",
            "28d_view": "64319",
            "action_type": "video_view",
            "value": "65698"
        }
    ],
    "video_p25_watched_actions": [
        {
            "1d_click": "1766",
            "1d_view": "161546",
            "28d_click": "1766",
            "28d_view": "161546",
            "action_type": "video_view",
            "value": "163312"
        }
    ],
    "video_p50_watched_actions": [
        {
            "1d_click": "1608",
            "1d_view": "104987",
            "28d_click": "1608",
            "28d_view": "104987",
            "action_type": "video_view",
            "value": "106595"
        }
    ],
    "video_p75_watched_actions": [
        {
            "1d_click": "1509",
            "1d_view": "81437",
            "28d_click": "1509",
            "28d_view": "81437",
            "action_type": "video_view",
            "value": "82946"
        }
    ],
    "video_p95_watched_actions": [
        {
            "1d_click": "1436",
            "1d_view": "68935",
            "28d_click": "1436",
            "28d_view": "68935",
            "action_type": "video_view",
            "value": "70371"
        }
    ]
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
    }
}


class InsightTransformationTest(TestCase):

    def test_basic_transformation(self):
        transformed = FieldTransformation.transform(data)
        self.maxDiff = None
        self.assertDictEqual(transformed, expected)
