# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

import uuid

from common.store.base import BaseMeta, BaseModel, attributes
from tests.base import random


class BaseModelTests(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # purposefully messing with real attr names to test .to_dict()
        class TestModel(BaseModel):
            Meta = BaseMeta(random.gen_string_id())
            primary_id = attributes.UnicodeAttribute(hash_key=True, attr_name='pid')
            secondary_id = attributes.UnicodeAttribute(range_key=True, attr_name='sid')
            data = attributes.UnicodeAttribute(null=True, attr_name='d')
            more_data = attributes.UnicodeAttribute(null=True, attr_name='d2')

        TestModel.create_table(wait=True)

        cls.Model = TestModel

    def test_base_model_to_dict(self):

        pid = random.gen_string_id()
        sid = random.gen_string_id()
        data = self.Model(pid, sid, data='primary data').to_dict()

        assert data == dict(
            primary_id=pid,
            secondary_id=sid,
            data='primary data',
            more_data=None
        )

    def test_base_model_upsert(self):

        pid = random.gen_string_id()
        sid = random.gen_string_id()

        # no record should exist, but upsert should succeed

        with self.assertRaises(self.Model.DoesNotExist):
            self.Model.get(pid, sid)

        m = self.Model.upsert(pid, sid, data='primary data')
        assert isinstance(m, self.Model)
        assert m.to_dict() == dict(
            primary_id=pid,
            secondary_id=sid,
            data='primary data',
            more_data=None
        )

        m = self.Model.get(pid, sid)
        assert m.to_dict() == dict(
            primary_id=pid,
            secondary_id=sid,
            data='primary data',
            more_data=None
        )

        # Now let's update same record and ensure we don't clobber
        # data we do NOT communicate in upsert

        # note that we don't pass in value for `data` attr
        m = self.Model.upsert(pid, sid, more_data='more data')
        assert isinstance(m, self.Model)
        assert m.to_dict() == dict(
            primary_id=pid,
            secondary_id=sid,
            data='primary data',  # <------- .update call picks up data that was already in DB
            more_data='more data'
        )

        # and just in case, fresh get

        m = self.Model.get(pid, sid)
        assert m.to_dict() == dict(
            primary_id=pid,
            secondary_id=sid,
            data='primary data',
            more_data='more data'
        )


class BaseModelToDictFieldsTests(TestCase):

    def test_additional_fields(self):

        # purposefully messing with real attr names to test .to_dict()
        class TestModel(BaseModel):

            _additional_fields = {'record_type'}

            Meta = BaseMeta(random.gen_string_id())

            primary_id = attributes.UnicodeAttribute(hash_key=True, attr_name='pid')
            secondary_id = attributes.UnicodeAttribute(range_key=True, attr_name='sid')
            data = attributes.UnicodeAttribute(null=True, attr_name='d')

            record_type = 'SUPER_RECORD'

        pid = random.gen_string_id()
        sid = random.gen_string_id()

        # ARG!!! for some reason instantiation further below
        # needs to have the table actually exist. Nuts!
        # TODO: fix this shit and allow object instantiation NOT require a hit to DB
        TestModel.create_table()
        record = TestModel(pid, sid, data='value')

        assert record.to_dict() == dict(
            primary_id=pid,
            secondary_id=sid,
            data='value',
            record_type='SUPER_RECORD'  # <--- note static attribute
        )

        assert record.to_dict(fields=['data', 'record_type']) == dict(
            # primary_id=pid,
            # secondary_id=sid,
            data='value',
            record_type='SUPER_RECORD'  # <--- note static attribute
        )

        with self.assertRaises(AttributeError) as ex:
            record.to_dict(fields=['data', 'does_not_exist'])

        error_message = str(ex.exception)
        assert 'object has no attribute' in error_message
        assert 'does_not_exist' in error_message

    def test_fixed_fields(self):

        # purposefully messing with real attr names to test .to_dict()
        class TestModel(BaseModel):

            _fields = {
                'secondary_id',
                'record_type'
            }

            Meta = BaseMeta(random.gen_string_id())

            primary_id = attributes.UnicodeAttribute(hash_key=True, attr_name='pid')
            secondary_id = attributes.UnicodeAttribute(range_key=True, attr_name='sid')
            data = attributes.UnicodeAttribute(null=True, attr_name='d')

            record_type = 'SUPER_RECORD'

        pid = random.gen_string_id()
        sid = random.gen_string_id()

        # ARG!!! for some reason instantiation further below
        # needs to have the table actually exist. Nuts!
        # TODO: fix this shit and allow object instantiation NOT require a hit to DB
        TestModel.create_table()
        record = TestModel(pid, sid, data='value')

        assert record.to_dict() == dict(
            # primary_id=pid,
            secondary_id=sid,
            # data='value',
            record_type='SUPER_RECORD'  # <--- note static attribute
        )

        assert record.to_dict(fields=['data', 'record_type']) == dict(
            # primary_id=pid,
            # secondary_id=sid,
            data='value',
            record_type='SUPER_RECORD'  # <--- note static attribute
        )
