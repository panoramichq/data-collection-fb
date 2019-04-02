from typing import Dict, Tuple, Iterable, Optional

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

    @property
    def is_leaf(self) -> bool:
        """Is the node a leaf node."""
        return not bool(self._children)

    def has_child(self, entity_id: str) -> bool:
        """Is entity_id of one of the children."""
        return entity_id in self._children

    def get_child(self, entity_id: str) -> Optional['EntityNode']:
        """Return child with given entity_id."""
        return self._children.get(entity_id)

    def add_node(self, node: 'EntityNode', path: Tuple[str, ...] = None):
        """Add child to node or descendants of node based on path."""
        if path is None or path == ():
            if self.is_leaf:
                self._children = {}
            self._children[node.entity_id] = node
            return

        insert_node = self
        for path_entity_id in path:
            if insert_node.is_leaf or not insert_node.has_child(path_entity_id):
                insert_node.add_node(EntityNode(path_entity_id, Entity.next_level(insert_node.entity_type)))
            insert_node = insert_node.get_child(path_entity_id)
        insert_node.add_node(node)
