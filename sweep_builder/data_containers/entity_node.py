from typing import Dict, Tuple, Iterable

from common.enums.entity import Entity


class EntityNode:

    entity_id: str
    entity_type: str
    _children: Dict[str, 'EntityNode'] = None

    def __init__(self, entity_id: str, entity_type: str):
        self.entity_id = entity_id
        self.entity_type = entity_type

    @property
    def children(self) -> Iterable['EntityNode']:
        """Child nodes of current node."""
        return self._children.values()

    def add_node(self, node: 'EntityNode', path: Tuple[str, ...] = None):
        """Add child to node or descendants of node based on path."""
        if path is None or path == ():
            if self._children is None:
                self._children = {}
            self._children[node.entity_id] = node
            return

        insert_node = self
        for path_entity_id in path:
            if insert_node._children is None or path_entity_id not in insert_node._children:
                insert_node.add_node(EntityNode(path_entity_id, Entity.next_level(insert_node.entity_type)))
            insert_node = insert_node._children[path_entity_id]
        insert_node.add_node(node)
