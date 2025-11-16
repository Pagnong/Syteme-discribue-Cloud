import time
import math
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Union, Tuple
from enum import Enum, auto
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict


class TransferStatus(Enum):
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()


@dataclass
class FileChunk:
    chunk_id: int
    size: int  # bytes
    checksum: str
    status: TransferStatus = TransferStatus.PENDING
    stored_node: Optional[str] = None
    created_at: float = time.time()
    # placeholder for data reference (not actual bytes here)
    data_ref: Optional[str] = None


@dataclass
class FileTransfer:
    file_id: str
    file_name: str
    total_size: int
    chunks: List[FileChunk]
    status: TransferStatus = TransferStatus.PENDING
    created_at: float = time.time()
    completed_at: Optional[float] = None


class LRUChunkCache:
    """
    Simple LRU cache for chunk metadata / small chunk content references.
    Stores up to `max_entries` items.
    """
    def __init__(self, max_entries: int = 128):
        self.max_entries = max_entries
        self._cache: "OrderedDict[Tuple[str,int], dict]" = OrderedDict()

    def get(self, file_id: str, chunk_id: int):
        key = (file_id, chunk_id)
        if key not in self._cache:
            return None
        # mark as recently used
        item = self._cache.pop(key)
        self._cache[key] = item
        return item

    def put(self, file_id: str, chunk_id: int, value: dict):
        key = (file_id, chunk_id)
        if key in self._cache:
            self._cache.pop(key)
        elif len(self._cache) >= self.max_entries:
            # evict least recently used
            self._cache.popitem(last=False)
        self._cache[key] = value

    def clear(self):
        self._cache.clear()

    def keys(self):
        return list(self._cache.keys())


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
        self.bandwidth = bandwidth * 1_000_000  # Convert Mbps to bits per second

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

        # Dynamic system load (in %)
        self.cpu_usage = random.uniform(5, 15)
        self.memory_usage = random.uniform(10, 25)

        # Storage map: structured metadata for faster access and cleanup
        self.storage_map = {
            "files": {},     # file_id -> metadata (FileTransfer-like)
            "chunks": {},    # (file_id, chunk_id) -> chunk metadata dict
            "usage_bytes": 0,
            "last_cleanup": time.time()
        }

        # LRU cache for chunk metadata / small content references
        self.chunk_cache = LRUChunkCache(max_entries=128)

        # Parameters
        self.gc_interval = 60            # seconds between automatic cleanup checks (if invoked externally)
        self.chunk_cache_max_entries = 128

    # ----------------------------
    # Node & connections
    # ----------------------------
    def add_connection(self, node_id: str, bandwidth: int):
        """Add a network connection to another node"""
        self.connections[node_id] = bandwidth * 1_000_000  # Store in bits per second

    # ----------------------------
    # Chunk sizing (dynamic)
    # ----------------------------
    def _calculate_chunk_size(self, file_size: int) -> int:
        """
        Determine chunk size adaptively based on file size and current cpu/memory load.
        Returns size in bytes.
        """
        # Base heuristics by file size
        if file_size < 10 * 1024 * 1024:  # < 10MB
            base = 512 * 1024  # 512KB
        elif file_size < 100 * 1024 * 1024:  # < 100MB
            base = 2 * 1024 * 1024  # 2MB
        else:
            base = 10 * 1024 * 1024  # 10MB

        # Adjust based on current load: if loaded, lower chunk size to reduce per-chunk overhead
        load_factor = (self.cpu_usage / 100.0) * 0.6 + (self.memory_usage / 100.0) * 0.4
        # if load_factor high, reduce chunk size; else allow base or larger
        if load_factor > 0.7:
            multiplier = 0.25  # heavy load -> much smaller chunks
        elif load_factor > 0.5:
            multiplier = 0.5
        elif load_factor < 0.2:
            multiplier = 1.2  # very idle -> slightly larger
        else:
            multiplier = 1.0

        adaptive_size = int(base * multiplier)

        # Boundaries to avoid too small or too large chunks
        min_chunk = 128 * 1024       # 128KB
        max_chunk = 20 * 1024 * 1024  # 20MB
        return max(min_chunk, min(adaptive_size, max_chunk))

    # ----------------------------
    # Chunk & file metadata handling
    # ----------------------------
    def _generate_chunks(self, file_id: str, file_size: int) -> List[FileChunk]:
        """Break file into chunks for transfer and register them in storage_map"""
        chunk_size = self._calculate_chunk_size(file_size)
        num_chunks = math.ceil(file_size / chunk_size)

        chunks: List[FileChunk] = []
        for i in range(num_chunks):
            fake_checksum = hashlib.md5(f"{file_id}-{i}".encode()).hexdigest()
            actual_chunk_size = min(chunk_size, file_size - i * chunk_size)
            chunk = FileChunk(
                chunk_id=i,
                size=actual_chunk_size,
                checksum=fake_checksum,
                status=TransferStatus.PENDING,
                stored_node=None,
                created_at=time.time(),
                data_ref=None
            )
            chunks.append(chunk)

            # register chunk metadata in storage_map
            self.storage_map["chunks"][(file_id, i)] = {
                "chunk_id": i,
                "size": actual_chunk_size,
                "checksum": fake_checksum,
                "status": TransferStatus.PENDING.name,
                "created_at": time.time(),
                "stored_node": None
            }

        # register file-level metadata
        self.storage_map["files"][file_id] = {
            "file_id": file_id,
            "file_size": file_size,
            "num_chunks": len(chunks),
            "created_at": time.time(),
            "status": TransferStatus.PENDING.name
        }
        return chunks

    # ----------------------------
    # Initiate transfer (no change in API)
    # ----------------------------
    def initiate_file_transfer(
        self,
        file_id: str,
        file_name: str,
        file_size: int,
        source_node: Optional[str] = None
    ) -> Optional[FileTransfer]:
        """Initiate a file storage request to this node"""
        # Check if we have enough storage space (considering overhead)
        if self.used_storage + file_size > self.total_storage:
            return None

        chunks = self._generate_chunks(file_id, file_size)
        transfer = FileTransfer(
            file_id=file_id,
            file_name=file_name,
            total_size=file_size,
            chunks=chunks,
            status=TransferStatus.PENDING,
            created_at=time.time()
        )

        self.active_transfers[file_id] = transfer
        return transfer

    # ----------------------------
    # LRU cache helpers
    # ----------------------------
    def _cache_get_chunk_meta(self, file_id: str, chunk_id: int) -> Optional[dict]:
        """Return cached chunk metadata if present."""
        return self.chunk_cache.get(file_id, chunk_id)

    def _cache_put_chunk_meta(self, file_id: str, chunk_id: int, meta: dict):
        """Put chunk metadata into LRU cache."""
        self.chunk_cache.put(file_id, chunk_id, meta)

    # ----------------------------
    # Simulate chunk transfer (keeps same semantics)
    # ----------------------------
    def _simulate_chunk_transfer(self, chunk: FileChunk, source_node: str) -> bool:
        """Simulate the transfer of a single chunk considering network and CPU/memory load"""
        # update load to reflect ongoing activity
        self._update_system_load()

        available_bandwidth = min(
            self.bandwidth - self.network_utilization,
            self.connections.get(source_node, 0)
        )

        if available_bandwidth <= 0:
            return False

        # Apply random network variation and CPU/memory penalty
        available_bandwidth *= random.uniform(0.7, 1.0)
        available_bandwidth *= self._performance_penalty()

        transfer_time = (chunk.size * 8) / available_bandwidth
        # accelerate simulation factor to avoid very long sleeps
        time.sleep(max(transfer_time / 10, 0.0001))

        # Mark chunk completed
        chunk.status = TransferStatus.COMPLETED
        chunk.stored_node = self.node_id
        chunk.created_at = chunk.created_at  # keep original
        self.total_data_transferred += chunk.size

        # Update metadata maps and LRU cache
        key = (chunk.chunk_id, chunk.checksum)
        # Update storage_map
        self.storage_map["chunks"][(chunk.file_id if hasattr(chunk, "file_id") else chunk.checksum, chunk.chunk_id)] = {
            "chunk_id": chunk.chunk_id,
            "size": chunk.size,
            "checksum": chunk.checksum,
            "status": TransferStatus.COMPLETED.name,
            "created_at": chunk.created_at,
            "stored_node": self.node_id
        }

        # Put into cache a small metadata dict (no real data)
        meta = {
            "file_id": getattr(chunk, "file_id", None),
            "chunk_id": chunk.chunk_id,
            "size": chunk.size,
            "checksum": chunk.checksum,
            "stored_node": self.node_id,
            "completed_at": time.time()
        }
        # best-effort cache put: if file_id not present, use placeholder
        fid = getattr(chunk, "file_id", None)
        if fid is not None:
            self._cache_put_chunk_meta(fid, chunk.chunk_id, meta)

        # small CPU/memory increase per chunk
        self.cpu_usage = min(100, self.cpu_usage + 0.2)
        self.memory_usage = min(100, self.memory_usage + 0.1)

        return True

    def process_chunks_parallel(self, file_id: str, source_node: str, max_workers: int = 4) -> int:
        """Process multiple chunks in parallel"""
        if file_id not in self.active_transfers:
            return 0

        transfer = self.active_transfers[file_id]
        pending_chunks = [c for c in transfer.chunks if c.status != TransferStatus.COMPLETED]

        if not pending_chunks:
            return 0

        chunks_transferred = 0
        # Attach file_id attribute to chunks to help map updates/cache
        for c in pending_chunks:
            setattr(c, "file_id", file_id)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._simulate_chunk_transfer, c, source_node): c
                for c in pending_chunks[:max_workers]
            }
            for f in as_completed(futures):
                try:
                    if f.result():
                        chunks_transferred += 1
                except Exception:
                    self.failed_transfers += 1

        # If transfer completed, finalize
        if all(c.status == TransferStatus.COMPLETED for c in transfer.chunks):
            transfer.status = TransferStatus.COMPLETED
            transfer.completed_at = time.time()
            self.used_storage += transfer.total_size
            self.stored_files[file_id] = transfer
            # update storage_map file status
            if file_id in self.storage_map["files"]:
                self.storage_map["files"][file_id]["status"] = TransferStatus.COMPLETED.name
            # update global usage
            self.storage_map["usage_bytes"] = self.used_storage
            # remove from active_transfers
            del self.active_transfers[file_id]
            self.total_requests_processed += 1

        return chunks_transferred

    # ----------------------------
    # Retrieve file (reads) — uses cache if possible
    # ----------------------------
    def retrieve_file(self, file_id: str, destination_node: str) -> Optional[FileTransfer]:
        """Initiate file retrieval to another node (keeps API)."""
        if file_id not in self.stored_files:
            return None

        file_transfer = self.stored_files[file_id]

        # Create new transfer record for retrieval (chunks metadata referencing stored chunks)
        new_transfer = FileTransfer(
            file_id=f"retr-{file_id}-{time.time()}",
            file_name=file_transfer.file_name,
            total_size=file_transfer.total_size,
            chunks=[
                FileChunk(
                    chunk_id=c.chunk_id,
                    size=c.size,
                    checksum=c.checksum,
                    status=TransferStatus.PENDING,
                    stored_node=destination_node,
                    created_at=time.time()
                )
                for c in file_transfer.chunks
            ]
        )

        return new_transfer

    # ----------------------------
    # System load simulation
    # ----------------------------
    def _update_system_load(self, delta_cpu: float = 0.5, delta_mem: float = 0.3):
        """Simulate CPU and memory load variation during transfers"""
        self.cpu_usage += random.uniform(-delta_cpu, delta_cpu)
        self.memory_usage += random.uniform(-delta_mem, delta_mem)
        self.cpu_usage = max(5, min(100, self.cpu_usage))
        self.memory_usage = max(10, min(100, self.memory_usage))

    def _performance_penalty(self) -> float:
        """Return a performance degradation factor based on CPU/memory load"""
        penalty = 1.0
        if self.cpu_usage > 80:
            penalty *= 0.7  # slowdown
        if self.memory_usage > 75:
            penalty *= 0.8
        return penalty

    # ----------------------------
    # Garbage collector & cleanup
    # ----------------------------
    def cleanup(self, max_age_seconds: int = 3600):
        """
        Clean up failed or very old transfers and chunk metadata that are stale.
        - Remove active transfers that are FAILED or older than max_age_seconds and not completed.
        - Remove chunk entries in storage_map that are still PENDING for too long.
        """
        now = time.time()
        removed_transfers = []

        # Clean active transfers
        for file_id, transfer in list(self.active_transfers.items()):
            age = now - transfer.created_at
            if transfer.status == TransferStatus.FAILED or age > max_age_seconds:
                # free any reserved space (since we didn't reserve bytes earlier, we simply delete metadata)
                removed_transfers.append(file_id)
                del self.active_transfers[file_id]
                # update storage_map
                if file_id in self.storage_map["files"]:
                    del self.storage_map["files"][file_id]
                # remove related chunks metadata
                for key in list(self.storage_map["chunks"].keys()):
                    if key[0] == file_id:
                        del self.storage_map["chunks"][key]

        # Clean stale chunks (pending too long)
        for key, meta in list(self.storage_map["chunks"].items()):
            if isinstance(meta.get("created_at"), (int, float)):
                if now - meta["created_at"] > max_age_seconds and meta["status"] == TransferStatus.PENDING.name:
                    del self.storage_map["chunks"][key]

        # Optionally clear old cache entries (no strict rule here)
        # If cache very large, trim it
        if len(self.chunk_cache.keys()) > self.chunk_cache_max_entries:
            self.chunk_cache.clear()

        # update last cleanup time
        self.storage_map["last_cleanup"] = now

        return removed_transfers

    # ----------------------------
    # Metrics & utils
    # ----------------------------
    def get_storage_utilization(self) -> Dict[str, Union[int, float, List[str]]]:
        """Get current storage utilization metrics"""
        return {
            "used_bytes": self.used_storage,
            "total_bytes": self.total_storage,
            "utilization_percent": (self.used_storage / self.total_storage) * 100 if self.total_storage else 0.0,
            "files_stored": len(self.stored_files),
            "active_transfers": len(self.active_transfers),
            "cpu_usage_percent": round(self.cpu_usage, 2),
            "memory_usage_percent": round(self.memory_usage, 2)
        }

    def get_network_utilization(self) -> Dict[str, Union[int, float, List[str]]]:
        """Get current network utilization metrics"""
        total_bandwidth_bps = self.bandwidth
        return {
            "current_utilization_bps": self.network_utilization,
            "max_bandwidth_bps": total_bandwidth_bps,
            "utilization_percent": (self.network_utilization / total_bandwidth_bps) * 100 if total_bandwidth_bps else 0,
            "connections": list(self.connections.keys())
        }

    def get_performance_metrics(self) -> Dict[str, int]:
        """Get node performance metrics"""
        return {
            "total_requests_processed": self.total_requests_processed,
            "total_data_transferred_bytes": self.total_data_transferred,
            "failed_transfers": self.failed_transfers,
            "current_active_transfers": len(self.active_transfers)
        }
