import json
from enum import Enum
from queue import Queue
from typing import Dict

import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_pydot import graphviz_layout


class GeoEntry:
    def __init__(self, region, postcode, city, street, number, unit):
        self.region = region
        self.postcode = postcode
        self.city = city
        self.street = street
        self.number = number
        self.unit = unit

    @staticmethod
    def from_properties(properties):
        region = GeoEntry.get_entry(properties, "region")
        postcode = GeoEntry.get_entry(properties, "postcode")
        city = GeoEntry.get_entry(properties, "city")
        street = GeoEntry.get_entry(properties, "street")
        number = GeoEntry.get_entry(properties, "number")
        unit = GeoEntry.get_entry(properties, "unit")
        return GeoEntry(region, postcode, city, street, number, unit)

    @staticmethod
    def get_entry(target_dict, target_entry):
        if target_entry in target_dict:
            target_value = target_dict[target_entry]
            target_value = target_value.strip()
            if target_value:
                return target_value
        return None

    def is_valid(self):
        return self.region is not None and self.postcode is not None and self.city is not None and self.street is not None

    def __repr__(self):
        return str(self.__dict__)


class GeoNode:
    class NodeType(Enum):
        # Hierarchy is:
        # Planet -> Country -> Region -> City -> Postcode -> Street -> Number -> Unit

        PLANET = 0
        COUNTRY = 1
        REGION = 2
        CITY = 3
        POSTCODE = 4
        STREET = 5
        NUMBER = 6
        UNIT = 7

    def __init__(self, label: str = None, node_type: NodeType = None):
        self.label = label
        self.node_type: GeoNode.NodeType = node_type
        self.children: Dict[str, GeoNode] = dict()

    def __hash__(self):
        return hash((self.label, self.node_type.value))

    def __repr__(self):
        return self.label


class GeoGraph:
    def __init__(self, root):
        self.root: GeoNode = root

    def add_child(self, child_entry: GeoEntry):
        if not child_entry.is_valid():
            raise ValueError(f"Geo entry is missing data. Entry: {child_entry}")

        cur_node = None
        # Use enum values as a state machine. Advance through all levels in sequence
        for node_type in GeoNode.NodeType:
            name = node_type.name
            level = node_type.value
            if node_type == GeoNode.NodeType.PLANET:
                # We assume all are on earth
                cur_node = self.root
            elif node_type == GeoNode.NodeType.COUNTRY:
                # We assume all are in USA
                cur_node = self.root.children["United States of America"]
            elif node_type == GeoNode.NodeType.REGION:
                cur_node = self._add_child(parent_node=cur_node, child_label=child_entry.region,
                                           child_type=GeoNode.NodeType.REGION)
            elif node_type == GeoNode.NodeType.CITY:
                cur_node = self._add_child(parent_node=cur_node, child_label=child_entry.city,
                                           child_type=GeoNode.NodeType.CITY)
            elif node_type == GeoNode.NodeType.POSTCODE:
                cur_node = self._add_child(parent_node=cur_node, child_label=child_entry.postcode,
                                           child_type=GeoNode.NodeType.POSTCODE)
            elif node_type == GeoNode.NodeType.STREET:
                cur_node = self._add_child(parent_node=cur_node, child_label=child_entry.street,
                                           child_type=GeoNode.NodeType.STREET)
            elif node_type == GeoNode.NodeType.NUMBER:
                if child_entry.number is None:
                    return  # If there's no number just ignore
                cur_node = self._add_child(parent_node=cur_node, child_label=child_entry.number,
                                           child_type=GeoNode.NodeType.NUMBER)
            elif node_type == GeoNode.NodeType.UNIT:
                if child_entry.unit is None:
                    return  # If there's no unit just ignore
                cur_node = self._add_child(parent_node=cur_node, child_label=child_entry.unit,
                                           child_type=GeoNode.NodeType.UNIT)

    @staticmethod
    def _add_child(parent_node: GeoNode, child_label: str, child_type: GeoNode.NodeType):
        child_node = parent_node.children.get(child_label, None)
        if child_node is None:
            child_node = GeoNode(label=child_label, node_type=child_type)
            parent_node.children[child_node.label] = child_node
        return child_node


num_incomplete = 0
num_processed = 0
lines = 100
if __name__ == '__main__':
    g = GeoGraph(root=GeoNode(label="Earth", node_type=GeoNode.NodeType.PLANET))
    country_node = GeoNode("United States of America", GeoNode.NodeType.COUNTRY)
    g.root.children[country_node.label] = country_node
    with open("./data/us_ca_san_diego-addresses-county.geojson", "r") as f:
        for line in f.readlines():
            if lines == 0:
                break
            lines -= 1
            line = line.strip()
            entry = json.loads(line)
            if "properties" in entry:
                geo_entry = GeoEntry.from_properties(entry["properties"])
                num_processed += 1
                try:
                    g.add_child(geo_entry)
                except Exception as e:
                    num_incomplete += 1
    print(f"Processed {num_processed} entries. {num_incomplete} were missing data and were skipped.")

    q = Queue()
    q.put(g.root)
    nx_graph = nx.DiGraph()
    # breadth first traversal of graph
    # Get the parent node, then add an edge to each of its children. Then traverse all the children
    while not q.empty():
        item = q.get()
        children = item.children.values()
        for child in children:
            nx_graph.add_edge(item, child)
            q.put(child)

    fig = plt.figure(1, figsize=(24, 24))
    pos = graphviz_layout(nx_graph, prog="dot")
    nx.draw(nx_graph, pos, with_labels=True, node_size=10, font_size=8)
    plt.savefig("geo_graph.png")
    plt.show()
