# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase
from datetime import datetime, timezone

from facebookads.adobjects import campaign, adset, ad
from config import test as test_config
from common.store import entities
from common import entity_types

from oozer.tasks import feedback_entity


class TestEntityFeedback(TestCase):

    KNOWN_ENETITY_ID = '123'
    KNOWN_ADACCOUNT_ID = test_config.FB_ADACCOUNT_ID

    def tearDown(self):
        """
        Drop all entities after tests
        """
        all_tables = [
            entities.FacebookCampaignEntity, entities.FacebookAdsetEntity,
            entities.FacebookAdEntity
        ]
        for table in all_tables:
            for row in table.scan():
                row.delete()

    def _entity_factory(
        self, entity_klazz, entity_id=KNOWN_ENETITY_ID,
            ad_account_id=KNOWN_ADACCOUNT_ID, **kwargs
    ):
        """
        Manufactures an entity (based on suppplied entity_klazz) that we can
        use for testing

        :param class entity_klazz: the
        :param dict **kwargs: Individual field values valid for given entity

        :return AbstractCrudObject: The manufactured entity
        """
        entity = entity_klazz(entity_id)
        entity[entity.Field.account_id] = ad_account_id

        entity_fields = filter(
            lambda v: not v.startswith('__'), dir(entity_klazz.Field)
        )

        # Add additional fields, if any
        for field in filter(lambda f: f in kwargs, entity_fields):
            entity[field] = kwargs[field]

        return entity

    def _feedback_entity(self, entity, entity_hash=None):
        """
        Runs the actual entity feedback task

        :param entity:
        :param entity_hash:
        :return:
        """
        feedback_entity.delay(
            entity_types.get_entity_name_from_fb_object(entity),
            dict(entity), entity_hash or ('e_hash', 'f_hash')
        ).get()

    def _lookup_default_entity(self, entity_model_klazz):
        """
        Attempt to lookup an entity item in Dynamo using the known hash and
        range keys

        :param class entity_model_klazz: Pynamo model class
        :return BaseModel: The fetched model

        :raises DoesNotExist: The item was not found in Dynam
        """
        return entity_model_klazz.get(
            hash_key=self.KNOWN_ADACCOUNT_ID,
            range_key=self.KNOWN_ENETITY_ID
        )

    def test_entity_reported(self):
        """
        Check that the entity ends up in Dynamo
        """
        mock_campaign = self._entity_factory(campaign.Campaign)

        # Run the task
        self._feedback_entity(mock_campaign, ('e_hash', 'f_hash'))

        # Lookup in Dynamo
        result = self._lookup_default_entity(entities.FacebookCampaignEntity)

        assert result.to_dict() == {
            'ad_account_id': self.KNOWN_ADACCOUNT_ID,
            'entity_id': self.KNOWN_ENETITY_ID,
            'bol': None,
            'eol': None,
            'hash': 'e_hash',
            'hash_fields': 'f_hash'
        }

    def test_upsert(self):
        """
        Ensure that multiple writes to the same key do not add a new record but
        just update the value(s)
        """
        mock_campaign = self._entity_factory(campaign.Campaign)

        # Run the task twice, second time we change the hash
        self._feedback_entity(mock_campaign)
        self._feedback_entity(mock_campaign, ('e_hash2', 'f_hash'))

        assert entities.FacebookCampaignEntity.count() == 1

        # Lookup in Dynamo
        result = self._lookup_default_entity(entities.FacebookCampaignEntity)

        assert result.to_dict() == {
            'ad_account_id': self.KNOWN_ADACCOUNT_ID,
            'entity_id': self.KNOWN_ENETITY_ID,
            'bol': None,
            'eol': None,
            'hash': 'e_hash2',
            'hash_fields': 'f_hash'
        }

    def test_bol_eol(self):
        """
        Test proper beginning of life / end of life behavior

        - created time should be recorded as beginning of life,
        - end of life will be recorded when campaign is in archived mode
        - the times must be in UTC
        """
        mock_campaign = self._entity_factory(
            campaign.Campaign,
            created_time="2018-02-14T16:33:19-0800",
            effective_status='ARCHIVED',
            updated_time='2018-02-17T16:33:19-0800'
        )

        self._feedback_entity(mock_campaign)

        result = self._lookup_default_entity(entities.FacebookCampaignEntity)

        assert result.to_dict() == {
            'ad_account_id': self.KNOWN_ADACCOUNT_ID,
            'entity_id': self.KNOWN_ENETITY_ID,
            'bol': datetime(
                year=2018, month=2, day=15, hour=0, minute=33, second=19,
                tzinfo=timezone.utc
            ),
            'eol': datetime(
                year=2018, month=2, day=18, hour=0, minute=33, second=19,
                tzinfo=timezone.utc
            ),
            'hash': 'e_hash',
            'hash_fields': 'f_hash'
        }

    def test_all_types_upserted(self):
        """
        Check that all entity types get inserted as expected
        """
        mock_campaign = self._entity_factory(campaign.Campaign)
        mock_adset = self._entity_factory(adset.AdSet)
        mock_ad = self._entity_factory(ad.Ad)

        all_mocks = [mock_campaign, mock_adset, mock_ad]

        # Feedback all
        for mock_entity in all_mocks:
            self._feedback_entity(mock_entity)

        all_models = [
            entities.FacebookCampaignEntity,
            entities.FacebookAdsetEntity,
            entities.FacebookAdEntity
        ]

        # Test all
        for model in all_models:
            result = self._lookup_default_entity(model)
            assert result.to_dict() == {
                'ad_account_id': self.KNOWN_ADACCOUNT_ID,
                'entity_id': self.KNOWN_ENETITY_ID,
                'bol': None,
                'eol': None,
                'hash': 'e_hash',
                'hash_fields': 'f_hash'
            }
            assert model.count() == 1

