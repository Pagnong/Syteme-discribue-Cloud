import time
from typing import Optional, Tuple
from node.storage_virtual_node import StorageVirtualNode, FileTransfer, TransferStatus
from core.persistence import PersistenceManager

class TransferManager:
    """
    Central manager responsible for handling all file transfer operations
    between nodes in the virtual storage network.
    """

    def __init__(self, network):
        self.network = network  # reference to StorageVirtualNetwork
        self.persistence = PersistenceManager()  # for saving history

    def initiate_transfer(
        self,
        source_node_id: str,
        target_node_id: str,
        file_name: str,
        file_size: int
    ) -> Optional[FileTransfer]:
        """Initialize and register a new transfer operation."""
        if source_node_id not in self.network.nodes or target_node_id not in self.network.nodes:
            return None

        target_node = self.network.nodes[target_node_id]
        transfer = target_node.initiate_file_transfer(
            file_id=self.network._generate_file_id(file_name),
            file_name=file_name,
            file_size=file_size,
            source_node=source_node_id
        )

        if transfer:
            self.network.transfer_operations[source_node_id][transfer.file_id] = transfer
            self.network._emit("transfer_started", {
                "source": source_node_id,
                "target": target_node_id,
                "file_name": file_name,
                "file_size": file_size
            })
        return transfer

    def process_transfer(
        self,
        source_node_id: str,
        target_node_id: str,
        file_id: str,
        chunks_per_step: int = 3
    ) -> Tuple[int, bool]:
        """Handle chunk transfers (parallelized)"""
        if (
            source_node_id not in self.network.nodes
            or target_node_id not in self.network.nodes
            or file_id not in self.network.transfer_operations[source_node_id]
        ):
            return (0, False)

        source_node = self.network.nodes[source_node_id]
        target_node = self.network.nodes[target_node_id]
        transfer = self.network.transfer_operations[source_node_id][file_id]

        chunks_transferred = target_node.process_chunks_parallel(
            file_id=file_id,
            source_node=source_node_id,
            max_workers=chunks_per_step
        )

        # --- If the transfer is completed ---
        if transfer.status == TransferStatus.COMPLETED:
            self._save_transfer_to_history(source_node, target_node, transfer)
            self.network._emit("transfer_completed", {
                "file_id": file_id,
                "source": source_node_id,
                "target": target_node_id,
                "total_size": transfer.total_size
            })
            del self.network.transfer_operations[source_node_id][file_id]
            return (chunks_transferred, True)

        return (chunks_transferred, False)

    # --------------------------------------------------
    # Internal helper: save transfer info in JSON history
    # --------------------------------------------------
    def _save_transfer_to_history(self, source_node, target_node, transfer: FileTransfer):
        """Save a completed transfer into persistent storage."""
        duration = transfer.completed_at - transfer.created_at if transfer.completed_at else 0
        node_stats = target_node.get_storage_utilization()

        record = {
            "file_id": transfer.file_id,
            "file_name": transfer.file_name,
            "source_node": source_node.node_id,
            "target_node": target_node.node_id,
            "file_size_MB": round(transfer.total_size / (1024 * 1024), 2),
            "transfer_duration_sec": round(duration, 2),
            "cpu_usage_end_percent": node_stats["cpu_usage_percent"],
            "memory_usage_end_percent": node_stats["memory_usage_percent"]
        }

        self.persistence.save_transfer_record(record)
