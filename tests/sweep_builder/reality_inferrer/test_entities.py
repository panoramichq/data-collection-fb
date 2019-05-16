import uuid
import pytest

from common.enums.entity import Entity
from config import dynamodb


@pytest.fixture
def patch_host_table_names(monkeypatch):
    test_id = uuid.uuid4()
    monkeypatch.setattr(dynamodb, 'HOST', 'http://localhost:8000')
    monkeypatch.setattr(dynamodb, 'AD_ENTITY_TABLE', f'AdEntity-{test_id}')
    monkeypatch.setattr(dynamodb, 'PAGE_POST_ENTITY_TABLE', f'PagePostEntity-{test_id}')
    monkeypatch.setattr(dynamodb, 'PAGE_VIDEO_ENTITY_TABLE', f'PageVideoEntity-{test_id}')


@pytest.fixture
def setup_ad_account_tables(patch_host_table_names):
    from common.store.entities import AdEntity

    AdEntity.create_table(wait=True)
    AdEntity(ad_account_id='ad-account-1', entity_id='ad-1', is_accessible=True).save()
    AdEntity(ad_account_id='ad-account-1', entity_id='ad-2', is_accessible=None).save()
    AdEntity(ad_account_id='ad-account-1', entity_id='ad-3', is_accessible=False).save()

    yield

    AdEntity.delete_table()


@pytest.fixture
def setup_page_tables(patch_host_table_names):
    from common.store.entities import PagePostEntity, PageVideoEntity

    PageVideoEntity.create_table(wait=True)
    PageVideoEntity(page_id='page-1', entity_id='page-video-1', is_accessible=True).save()
    PageVideoEntity(page_id='page-1', entity_id='page-video-2', is_accessible=None).save()
    PageVideoEntity(page_id='page-1', entity_id='page-video-3', is_accessible=False).save()

    PagePostEntity.create_table(wait=True)
    PagePostEntity(page_id='page-1', entity_id='page-post-1', is_accessible=True).save()
    PagePostEntity(page_id='page-1', entity_id='page-post-2', is_accessible=None).save()
    PagePostEntity(page_id='page-1', entity_id='page-post-3', is_accessible=False).save()

    yield

    PageVideoEntity.delete_table()
    PagePostEntity.delete_table()


def test_iter_entities_per_page_id(setup_page_tables):
    from sweep_builder.reality_inferrer.entities import iter_entities_per_page_id

    results = {r['entity_id'] for r in iter_entities_per_page_id('page-1')}

    assert results == {'page-post-1', 'page-post-2', 'page-video-1', 'page-video-2'}


def test_iter_entities_per_ad_account_id(setup_ad_account_tables):
    from sweep_builder.reality_inferrer.entities import iter_entities_per_ad_account_id

    results = {r['entity_id'] for r in iter_entities_per_ad_account_id('ad-account-1', entity_types=[Entity.Ad])}

    assert results == {'ad-1', 'ad-2'}
