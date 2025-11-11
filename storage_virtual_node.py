import time
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from enum import Enum, auto
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

class TransferStatus(Enum):
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()

@dataclass
class FileChunk:
    chunk_id: int
    size: int  # in bytes
    checksum: str
    status: TransferStatus = TransferStatus.PENDING
    stored_node: Optional[str] = None

@dataclass
class FileTransfer:
    file_id: str
    file_name: str
    total_size: int  # in bytes
    chunks: List[FileChunk]
    status: TransferStatus = TransferStatus.PENDING
    created_at: float = time.time()
    completed_at: Optional[float] = None

class StorageVirtualNode:
    def __init__(
        self,
        node_id: str,
        cpu_capacity: int,  # in vCPUs
        memory_capacity: int,  # in GB
        storage_capacity: int,  # in GB
        bandwidth: int  # in Mbps
    ):
        self.node_id = node_id
        self.cpu_capacity = cpu_capacity
        self.memory_capacity = memory_capacity
        self.total_storage = storage_capacity * 1024 * 1024 * 1024  # Convert GB to bytes
        self.bandwidth = bandwidth * 1000000  # Convert Mbps to bits per second
        
        # Current utilization
        self.used_storage = 0
        self.active_transfers: Dict[str, FileTransfer] = {}
        self.stored_files: Dict[str, FileTransfer] = {}
        self.network_utilization = 0  # Current bandwidth usage
        
        # Performance metrics
        self.total_requests_processed = 0
        self.total_data_transferred = 0  # in bytes
        self.failed_transfers = 0
        
        # Network connections (node_id: bandwidth_available)
        self.connections: Dict[str, int] = {}

    def add_connection(self, node_id: str, bandwidth: int):
        """Add a network connection to another node"""
        self.connections[node_id] = bandwidth * 1000000  # Store in bits per second

    def _calculate_chunk_size(self, file_size: int) -> int:
        """Determine optimal chunk size based on file size"""
        # Simple heuristic: larger files get larger chunks
        if file_size < 10 * 1024 * 1024:  # < 10MB
            return 512 * 1024  # 512KB chunks
        elif file_size < 100 * 1024 * 1024:  # < 100MB
            return 2 * 1024 * 1024  # 2MB chunks
        else:
            return 10 * 1024 * 1024  # 10MB chunks

    def _generate_chunks(self, file_id: str, file_size: int) -> List[FileChunk]:
        """Break file into chunks for transfer"""
        chunk_size = self._calculate_chunk_size(file_size)
        num_chunks = math.ceil(file_size / chunk_size)
        
        chunks = []
        for i in range(num_chunks):
            # In a real system, we'd compute actual checksums
            fake_checksum = hashlib.md5(f"{file_id}-{i}".encode()).hexdigest()
            actual_chunk_size = min(chunk_size, file_size - i * chunk_size)
            chunks.append(FileChunk(
                chunk_id=i,
                size=actual_chunk_size,
                checksum=fake_checksum
            ))
        
        return chunks

    def initiate_file_transfer(
        self,
        file_id: str,
        file_name: str,
        file_size: int,
        source_node: Optional[str] = None
    ) -> Optional[FileTransfer]:
        """Initiate a file storage request to this node"""
        # Check if we have enough storage space
        if self.used_storage + file_size > self.total_storage:
            return None
        
        # Create file transfer record
        chunks = self._generate_chunks(file_id, file_size)
        transfer = FileTransfer(
            file_id=file_id,
            file_name=file_name,
            total_size=file_size,
            chunks=chunks
        )
        
        self.active_transfers[file_id] = transfer
        return transfer

     def _simulate_chunk_transfer(
         self,
         chunk: FileChunk,
         source_node: str) -> bool:
        """Simulate the transfer of a single chunk (with dynamic bandwidth)"""
        available_bandwidth = min(
            self.bandwidth - self.network_utilization,
            self.connections.get(source_node, 0)
        )

        if available_bandwidth <= 0:
            return False

        # Simulate network variation (±30%)
        available_bandwidth *= random.uniform(0.7, 1.0)

        transfer_time = (chunk.size * 8) / available_bandwidth
        time.sleep(transfer_time / 10)  # Accelerated simulation

        chunk.status = TransferStatus.COMPLETED
        chunk.stored_node = self.node_id
        self.total_data_transferred += chunk.size

        return True

    def process_chunk_transfer(
        self,
        file_id: str,
        chunk_id: int,
        source_node: str
    ) -> bool:
        """Process an incoming file chunk"""
        if file_id not in self.active_transfers:
            return False
        
        transfer = self.active_transfers[file_id]
        chunk = next(c for c in transfer.chunks if c.chunk_id == chunk_id)
        if not chunk or chunk.status == TransferStatus.COMPLETED:
            return False

        success = self._simulate_chunk_transfer(chunk, source_node)
        if not success:
            self.failed_transfers += 1
            return False

        # Check completion
        if all(c.status == TransferStatus.COMPLETED for c in transfer.chunks):
            transfer.status = TransferStatus.COMPLETED
            transfer.completed_at = time.time()
            self.used_storage += transfer.total_size
            self.stored_files[file_id] = transfer
            del self.active_transfers[file_id]
            self.total_requests_processed += 1

        return True

    def process_chunks_parallel(self, file_id: str, source_node: str, max_workers: int = 4) -> int:
        """Process multiple chunks in parallel"""
        if file_id not in self.active_transfers:
            return 0

        transfer = self.active_transfers[file_id]
        pending_chunks = [c for c in transfer.chunks if c.status != TransferStatus.COMPLETED]

        chunks_transferred = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._simulate_chunk_transfer, c, source_node): c
                for c in pending_chunks[:max_workers]
            }
            for f in as_completed(futures):
                if f.result():
                    chunks_transferred += 1

        return chunks_transferred

    def get_storage_utilization(self):
        return {
            "used_bytes": self.used_storage,
            "total_bytes": self.total_storage,
            "utilization_percent": (self.used_storage / self.total_storage) * 100,
            "files_stored": len(self.stored_files),
            "active_transfers": len(self.active_transfers)
        }
