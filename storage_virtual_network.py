import hashlib
import time
from collections import defaultdict
from typing import Dict
from storage_virtual_node import StorageVirtualNode
from core.transfer_manager import TransferManager
from core.events import EventManager

class StorageVirtualNetwork:
    def __init__(self):
        self.nodes: Dict[str, StorageVirtualNode] = {}
        self.transfer_operations = defaultdict(dict)
        self.events = EventManager()
        self.transfers = TransferManager(self)

    # ----------------------
    # Node & Network Control
    # ----------------------
    def add_node(self, node: StorageVirtualNode):
        self.nodes[node.node_id] = node
        self._emit("node_added", {"node_id": node.node_id})

    def connect_nodes(self, node1_id: str, node2_id: str, bandwidth: int):
        if node1_id in self.nodes and node2_id in self.nodes:
            self.nodes[node1_id].add_connection(node2_id, bandwidth)
            self.nodes[node2_id].add_connection(node1_id, bandwidth)
            self._emit("nodes_connected", {
                "node1": node1_id,
                "node2": node2_id,
                "bandwidth": bandwidth
            })
            return True
        return False

    # ----------------------
    # Helper / internal
    # ----------------------
    def _generate_file_id(self, file_name: str) -> str:
        return hashlib.md5(f"{file_name}-{time.time()}".encode()).hexdigest()

    def _emit(self, event_name: str, data: dict):
        self.events.emit(event_name, data)

    # ----------------------
    # Stats
    # ----------------------
    def get_network_stats(self):
        total_bandwidth = sum(n.bandwidth for n in self.nodes.values())
        used_bandwidth = sum(n.network_utilization for n in self.nodes.values())
        total_storage = sum(n.total_storage for n in self.nodes.values())
        used_storage = sum(n.used_storage for n in self.nodes.values())

        return {
            "total_nodes": len(self.nodes),
            "total_bandwidth_bps": total_bandwidth,
            "used_bandwidth_bps": used_bandwidth,
            "bandwidth_utilization": (used_bandwidth / total_bandwidth) * 100 if total_bandwidth else 0,
            "storage_utilization": (used_storage / total_storage) * 100 if total_storage else 0,
            "active_transfers": sum(len(t) for t in self.transfer_operations.values())
        }
