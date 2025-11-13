import time
from typing import Optional, Tuple
from node.storage_virtual_node import StorageVirtualNode, FileTransfer, TransferStatus

class TransferManager:
    """
    Central manager responsible for handling all file transfer operations
    between nodes in the virtual storage network.
    """

    def __init__(self, network):
        self.network = network  # reference to StorageVirtualNetwork

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

        if transfer.status == TransferStatus.COMPLETED:
            self.network._emit("transfer_completed", {
                "file_id": file_id,
                "source": source_node_id,
                "target": target_node_id,
                "total_size": transfer.total_size
            })
            del self.network.transfer_operations[source_node_id][file_id]
            return (chunks_transferred, True)

        return (chunks_transferred, False)
