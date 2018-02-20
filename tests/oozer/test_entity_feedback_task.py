# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from datetime import datetime, timezone, timedelta

from common.facebook.entity_model_map import MODEL_ENTITY_TYPE_MAP as FB_MODEL_ENTITY_TYPE_MAP
from common.store.entities import ENTITY_TYPE_MODEL_MAP as ENTITY_TYPE_DB_MODEL_MAP
from facebookads.adobjects import campaign, adset, ad
from oozer.entities.tasks import feedback_entity_task
from tests.base.random import get_string_id


class TestEntityFeedback(TestCase):

    def _entity_factory(
        self,
        entity_klazz,
        ad_account_id=None,
        entity_id=None,
        **kwargs
    ):
        """
        Manufactures an entity (based on suppplied entity_klazz) that we can
        use for testing

        :param class entity_klazz: the
        :param dict **kwargs: Individual field values valid for given entity

        :return AbstractCrudObject: The manufactured entity
        """

        ad_account_id = ad_account_id or get_string_id()
        entity_id = entity_id or get_string_id()

        entity = entity_klazz(entity_id)
        entity[entity.Field.account_id] = ad_account_id

        entity_fields = filter(
            lambda v: not v.startswith('__'), dir(entity_klazz.Field)
        )

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

        aaid = get_string_id()
        eid = get_string_id()

        entity_data = dict(
            # returned value here is FB SDK model, hence the dict( above.
            self._entity_factory(
                FBModel,
                ad_account_id=aaid,
                entity_id=eid,
                created_time="2000-01-2T03:04:05-0800",
                configured_status='ARCHIVED',
                updated_time='2001-01-2T03:04:05-0800'
            )
        )

        feedback_entity_task(
            entity_data,
            entity_type,
            ('e_hash', 'f_hash')
        )

        record = DBModel.get(aaid, eid)

        assert record.to_dict() == {
            'ad_account_id': aaid,
            'entity_id': eid,
            'entity_type': entity_type,
            'bol': datetime(2000, 1, 2, 11, 4, 5, tzinfo=timezone.utc),
            'eol': datetime(2001, 1, 2, 11, 4, 5, tzinfo=timezone.utc),
            'hash': 'e_hash',
            'hash_fields': 'f_hash'
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
                updated_time=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S%z')  # <- later date
            )
        )

        feedback_entity_task(
            entity_data,
            entity_type,
            ('e_hash', 'f_hash')
        )

        record = DBModel.get(aaid, eid)

        assert record.to_dict() == {
            'ad_account_id': aaid,
            'entity_id': eid,
            'entity_type': entity_type,
            'bol': datetime(2000, 1, 2, 11, 4, 5, tzinfo=timezone.utc),  # <- original value
            'eol': datetime(2001, 1, 2, 11, 4, 5, tzinfo=timezone.utc),  # <- original value
            'hash': 'e_hash',
            'hash_fields': 'f_hash'
        }


    def test_all_upserted(self):
        """
        Check that all entity types get inserted as expected
        """
        for FBModel in [ad.Ad, adset.AdSet, campaign.Campaign]:

            entity_type = FB_MODEL_ENTITY_TYPE_MAP[FBModel]
            DBModel = ENTITY_TYPE_DB_MODEL_MAP[entity_type]

            aaid = get_string_id()
            eid = get_string_id()

            # created_time = datetime.utcnow() - timedelta(days=-5),
            # updated_time = datetime.utcnow()

            entity_data = dict(
                # returned value here is FB SDK model, hence the dict( above.
                self._entity_factory(
                    FBModel,
                    ad_account_id=aaid,
                    entity_id=eid,
                    # created_time=created_time,
                    # updated_time=updated_time,
                )
            )

            feedback_entity_task(
                entity_data,
                entity_type,
                ('e_hash', 'f_hash')
            )

            record = DBModel.get(aaid, eid)
            assert record.to_dict() == {
                'ad_account_id': aaid,
                'entity_id': eid,
                'entity_type': entity_type,
                'bol': None,
                'eol': None,
                'hash': 'e_hash',
                'hash_fields': 'f_hash'
            }
