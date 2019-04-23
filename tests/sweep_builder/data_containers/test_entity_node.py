from common.enums.entity import Entity
from sweep_builder.data_containers.entity_node import EntityNode


def test_add_child():
    node = EntityNode('adset-id', Entity.AdSet)

    node.add_node(EntityNode('ad-id1', Entity.Ad))

    assert ['ad-id1'] == [n.entity_id for n in node.children]


def test_add_child_with_path():
    node = EntityNode('ad-account-id', Entity.AdAccount)

    node.add_node(EntityNode('ad-id', Entity.Ad), ('campaign-id', 'adset-id'))

    campaign_node = list(node.children)[0]
    adset_node = list(campaign_node.children)[0]
    ad_node = list(adset_node.children)[0]

    assert 'campaign-id' == campaign_node.entity_id
    assert 'adset-id' == adset_node.entity_id
    assert 'ad-id' == ad_node.entity_id


def test_is_leaf():
    node = EntityNode('ad-account-id', Entity.AdAccount)
    assert node.is_leaf


def test_has_child():
    node = EntityNode('ad-account-id', Entity.AdAccount)
    node.add_node(EntityNode('ad-id1', Entity.Ad))

    assert node.has_child('ad-id1')


def test_get_child():
    node = EntityNode('ad-account-id', Entity.AdAccount)
    child = EntityNode('ad-id1', Entity.Ad)
    node.add_node(child)

    assert child == node.get_child('ad-id1')
