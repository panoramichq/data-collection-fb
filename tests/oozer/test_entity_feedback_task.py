# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

import pytest

from unittest import mock
from freezegun import freeze_time
from datetime import datetime, timezone

from common.enums.entity import Entity
from common.facebook.entity_model_map import MODEL_ENTITY_TYPE_MAP as FB_MODEL_ENTITY_TYPE_MAP
from common.store.entities import ENTITY_TYPE_MODEL_MAP as ENTITY_TYPE_DB_MODEL_MAP
from facebook_business.adobjects import campaign, adcreative, advideo, customaudience
from oozer.entities.tasks import feedback_entity_task
from tests.base.random import gen_string_id


class TestEntityFeedback(TestCase):
    def _entity_factory(self, entity_klazz, ad_account_id=None, entity_id=None, **kwargs):
        """
        Manufactures an entity (based on suppplied entity_klazz) that we can
        use for testing

        :param class entity_klazz: the
        :param dict **kwargs: Individual field values valid for given entity

        :return AbstractCrudObject: The manufactured entity
        """

        ad_account_id = ad_account_id or gen_string_id()
        entity_id = entity_id or gen_string_id()

        entity = entity_klazz(entity_id)

        if entity_klazz == advideo.AdVideo:
            entity['account_id'] = ad_account_id
        else:
            entity[entity.Field.account_id] = ad_account_id

        entity_fields = filter(lambda v: not v.startswith('__'), dir(entity_klazz.Field))

        # Add additional fields, if any
        for field in filter(lambda f: f in kwargs, entity_fields):
            entity[field] = kwargs[field]

        return entity

    def test_bol_eol(self):
        """
        Test proper beginning of life / end of life behavior

        - created time should be recorded as beginning of life,
        - end of life will be recorded when campaign is in archived mode
        - the times must be in UTC
        """

        FBModel = campaign.Campaign
        entity_type = FB_MODEL_ENTITY_TYPE_MAP[FBModel]
        DBModel = ENTITY_TYPE_DB_MODEL_MAP[entity_type]

        aaid = gen_string_id()
        eid = gen_string_id()

        entity_data = dict(
            # returned value here is FB SDK model, hence the dict( above.
            self._entity_factory(
                FBModel,
                ad_account_id=aaid,
                entity_id=eid,
                created_time="2000-01-2T03:04:05-0800",
                configured_status='ARCHIVED',
                updated_time='2001-01-2T03:04:05-0800',
            )
        )

        feedback_entity_task(entity_data, entity_type, ('e_hash', 'f_hash'))

        record = DBModel.get(aaid, eid)

        assert record.to_dict() == {
            'ad_account_id': aaid,
            'entity_id': eid,
            'entity_type': entity_type,
            'bol': datetime(2000, 1, 2, 11, 4, 5, tzinfo=timezone.utc),
            'eol': datetime(2001, 1, 2, 11, 4, 5, tzinfo=timezone.utc),
        }

        # Now testing retention of the original BOL, EOL values

        entity_data = dict(
            # returned value here is FB SDK model, hence the dict( above.
            self._entity_factory(
                FBModel,
                ad_account_id=aaid,
                entity_id=eid,
                created_time="1980-01-2T03:04:05-0800",  # <- earlier date
                configured_status='ARCHIVED',
                updated_time=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S%z'),  # <- later date
            )
        )

        feedback_entity_task(entity_data, entity_type, ('e_hash', 'f_hash'))

        record = DBModel.get(aaid, eid)

        assert record.to_dict() == {
            'ad_account_id': aaid,
            'entity_id': eid,
            'entity_type': entity_type,
            'bol': datetime(2000, 1, 2, 11, 4, 5, tzinfo=timezone.utc),  # <- original value
            'eol': datetime(2001, 1, 2, 11, 4, 5, tzinfo=timezone.utc),  # <- original value
        }

    def test_bol_translation(self):
        """
        Check that the default BOL values are populated on entities without creation date
        """

        FBModel = customaudience.CustomAudience
        entity_type = FB_MODEL_ENTITY_TYPE_MAP[FBModel]
        DBModel = ENTITY_TYPE_DB_MODEL_MAP[entity_type]

        aaid = gen_string_id()
        eid = gen_string_id()

        entity_data = dict(
            # returned value here is FB SDK model, hence the dict( above.
            self._entity_factory(FBModel, account_id=aaid, id=eid, time_created=1523049070, time_updated=1533162823)
        )

        feedback_entity_task(entity_data, entity_type, ('e_hash', 'f_hash'))

        record = DBModel.get(aaid, eid)

        assert record.to_dict() == {
            'ad_account_id': aaid,
            'entity_id': eid,
            'entity_type': entity_type,
            'bol': datetime(2018, 4, 6, 21, 11, 10, tzinfo=timezone.utc),
            'eol': None,
        }

    @freeze_time()
    def test_default_bol(self):
        """
        Check that the default BOL values are populated on entities without creation date
        """

        FBModel = adcreative.AdCreative
        entity_type = FB_MODEL_ENTITY_TYPE_MAP[FBModel]
        DBModel = ENTITY_TYPE_DB_MODEL_MAP[entity_type]

        aaid = gen_string_id()
        eid = gen_string_id()

        entity_data = dict(
            # returned value here is FB SDK model, hence the dict( above.
            self._entity_factory(FBModel, ad_account_id=aaid, id=eid)
        )

        feedback_entity_task(entity_data, entity_type, ('e_hash', 'f_hash'))

        record = DBModel.get(aaid, eid)

        assert record.to_dict() == {
            'ad_account_id': aaid,
            'entity_id': eid,
            'entity_type': entity_type,
            'bol': datetime.now(timezone.utc),
            'eol': None,
        }


@pytest.mark.parametrize(
    ['entity_type', 'entity_data', 'expected'],
    [
        (
            Entity.Campaign,
            {'created_time': '2019-01-01T12:00:00.000Z'},
            {'entity_type': Entity.Campaign, 'bol': datetime(2019, 1, 1, 12, 0, tzinfo=timezone.utc), 'eol': None},
        ),
        (
            Entity.AdSet,
            {'created_time': '2019-01-01T12:00:00.000Z', 'campaign_id': 'campaign-1'},
            {
                'entity_type': Entity.AdSet,
                'bol': datetime(2019, 1, 1, 12, 0, tzinfo=timezone.utc),
                'eol': None,
                'campaign_id': 'campaign-1',
            },
        ),
        (
            Entity.Ad,
            {'created_time': '2019-01-01T12:00:00.000Z', 'campaign_id': 'campaign-1', 'adset_id': 'adset-1'},
            {
                'entity_type': Entity.Ad,
                'bol': datetime(2019, 1, 1, 12, 0, tzinfo=timezone.utc),
                'eol': None,
                'campaign_id': 'campaign-1',
                'adset_id': 'adset-1',
            },
        ),
        (Entity.AdCreative, {}, {'entity_type': Entity.AdCreative, 'bol': mock.ANY, 'eol': None}),
        (Entity.AdVideo, {}, {'entity_type': Entity.AdVideo, 'bol': mock.ANY, 'eol': None}),
    ],
)
def test_all_upserted(entity_type, entity_data, expected):
    aaid = gen_string_id()
    eid = gen_string_id()

    entity_data.update(account_id=aaid, id=eid)
    expected.update(ad_account_id=aaid, entity_id=eid)

    feedback_entity_task(entity_data, entity_type, ('e_hash', 'f_hash'))

    record = ENTITY_TYPE_DB_MODEL_MAP[entity_type].get(entity_data['account_id'], entity_data['id'])
    assert record.to_dict() == expected
