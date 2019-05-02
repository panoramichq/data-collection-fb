from datetime import datetime, date
from unittest.mock import patch

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.job_signature import JobSignature
from sweep_builder.data_containers.entity_node import EntityNode
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.expectation_builder.expectations_inventory.metrics.breakdowns import (
    day_metrics_per_entity_under_ad_account,
)


@patch('sweep_builder.expectation_builder.expectations_inventory.metrics.breakdowns.iter_reality_per_ad_account_claim')
def test_day_metrics_per_entity_under_ad_account_not_divisible(mock_iter_reality_per_ad_account):
    reality_claim = RealityClaim(
        ad_account_id='ad-account-id',
        entity_id='ad-account-id',
        entity_type=Entity.AdAccount,
        timezone='America/Los_Angeles',
    )

    mock_iter_reality_per_ad_account.return_value = [
        RealityClaim(
            entity_type=Entity.Ad,
            campaign_id='campaign-1',
            adset_id='adset-1',
            entity_id='ad-1',
            bol=datetime(2019, 1, 1, 12, 0),
            eol=datetime(2019, 1, 2, 12, 0),
        ),
        RealityClaim(
            entity_type=Entity.Ad, entity_id='ad-2', bol=datetime(2019, 1, 3, 12, 0), eol=datetime(2019, 1, 3, 12, 0)
        ),
    ]

    result = list(day_metrics_per_entity_under_ad_account(Entity.Ad, [ReportType.day], reality_claim))

    assert result == [
        ExpectationClaim(
            'ad-account-id',
            Entity.AdAccount,
            ReportType.day,
            Entity.Ad,
            JobSignature('fb|ad-account-id|||day|A|2019-01-01'),
            ad_account_id='ad-account-id',
            timezone='America/Los_Angeles',
            range_start=date(2019, 1, 1),
        ),
        ExpectationClaim(
            'ad-account-id',
            Entity.AdAccount,
            ReportType.day,
            Entity.Ad,
            JobSignature('fb|ad-account-id|||day|A|2019-01-02'),
            ad_account_id='ad-account-id',
            timezone='America/Los_Angeles',
            range_start=date(2019, 1, 2),
        ),
        ExpectationClaim(
            'ad-account-id',
            Entity.AdAccount,
            ReportType.day,
            Entity.Ad,
            JobSignature('fb|ad-account-id|||day|A|2019-01-03'),
            ad_account_id='ad-account-id',
            timezone='America/Los_Angeles',
            range_start=date(2019, 1, 3),
        ),
    ]


@patch('sweep_builder.expectation_builder.expectations_inventory.metrics.breakdowns.iter_reality_per_ad_account_claim')
def test_day_metrics_per_entity_under_ad_account_is_divisible(mock_iter_reality_per_ad_account):
    reality_claim = RealityClaim(
        ad_account_id='ad-account-id',
        entity_id='ad-account-id',
        entity_type=Entity.AdAccount,
        timezone='America/Los_Angeles',
    )

    mock_iter_reality_per_ad_account.return_value = [
        RealityClaim(
            entity_type=Entity.Ad,
            campaign_id='campaign-1',
            adset_id='adset-1',
            entity_id='ad-1',
            bol=datetime(2019, 1, 1, 12, 0),
            eol=datetime(2019, 1, 1, 12, 0),
        )
    ]

    result = list(day_metrics_per_entity_under_ad_account(Entity.Ad, [ReportType.day], reality_claim))

    assert result == [
        ExpectationClaim(
            'ad-account-id',
            Entity.AdAccount,
            ReportType.day,
            Entity.Ad,
            JobSignature('fb|ad-account-id|||day|A|2019-01-01'),
            ad_account_id='ad-account-id',
            timezone='America/Los_Angeles',
            range_start=date(2019, 1, 1),
            entity_hierarchy=EntityNode(
                'ad-account-id',
                Entity.AdAccount,
                children=[
                    EntityNode(
                        'campaign-1',
                        Entity.Campaign,
                        children=[EntityNode('adset-1', Entity.AdSet, children=[EntityNode('ad-1', Entity.Ad)])],
                    )
                ],
            ),
        )
    ]
