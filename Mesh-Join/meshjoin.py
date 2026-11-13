"""
meshjoin.py — MESHJOIN algorithm with interleaved dual-pass enrichment.

Components:
1. StreamBuffer: Thread-safe bounded buffer for incoming stream transactions (capacity 1000).
2. Transaction: Data structure holding transaction attributes and enriched dimensions.
3. Chunk: Queue entry tracking arrival order and age (iterations in memory).
4. MeshJoinEngine: Interleaved dual-pass join algorithm:
   - Probes Customer Master Data (9 partitions of 500 tuples)
   - Probes Product Master Data (8 partitions of 500 tuples)
   - Evicts chunks after max(9, 8) = 9 iterations (seeing all partitions of both)
   - Master data is read in 500-tuple partitions from disk/DB, never loaded as a whole dictionary.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
import math
import sys
import threading
from typing import Any, Callable, Dict, List, Optional

from config import (
    PARTITION_SIZE,
    STREAM_BUFFER_CAPACITY,
    STREAM_CHUNK_SIZE,
    STORE_LIST,
)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Transaction Data Structure
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Transaction:
    transaction_id: str
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    event_time: Any
    sales_mode: str
    processing_mode: str = ""
    store_id: int = 1

    # Customer enrichment fields
    gender: Optional[str] = None
    age_group: Optional[str] = None
    occupation: Optional[str] = None
    city_category: Optional[str] = None
    stay_in_current_city_years: Optional[str] = None
    marital_status: Optional[str] = None

    # Product & Supplier enrichment fields
    product_name: Optional[str] = None
    product_category: Optional[str] = None
    supplier_id: Optional[str] = None
    supplier_name: Optional[str] = None

    # Store enrichment fields (from in-memory STORE_LIST)
    store_name: Optional[str] = None
    store_city: Optional[str] = None

    # Calculated fields
    revenue: float = 0.0

    # Enrichment status flags
    customer_enriched: bool = False
    product_enriched: bool = False

    def __post_init__(self):
        self.transaction_id = str(self.transaction_id)
        self.customer_id = str(self.customer_id)
        self.product_id = str(self.product_id)
        self.quantity = int(self.quantity)
        self.unit_price = float(self.unit_price)
        self.store_id = int(self.store_id)

        if not self.processing_mode:
            self.processing_mode = self.sales_mode

        if self.revenue == 0.0 and self.quantity and self.unit_price:
            self.revenue = round(self.quantity * self.unit_price, 2)

        self.enrich_store()

    def enrich_store(self) -> None:
        """Enrich store attributes from in-memory STORE_LIST."""
        if self.store_id in STORE_LIST:
            info = STORE_LIST[self.store_id]
            self.store_name = info.get("store_name")
            self.store_city = info.get("city")

    def is_fully_enriched(self) -> bool:
        """Return True if both customer and product passes enriched the transaction."""
        return self.customer_enriched and self.product_enriched

    def to_dict(self) -> Dict[str, Any]:
        """Convert transaction record to dictionary."""
        return {
            "transaction_id": self.transaction_id,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "event_time": self.event_time,
            "sales_mode": self.sales_mode,
            "processing_mode": self.processing_mode,
            "store_id": self.store_id,
            "gender": self.gender,
            "age_group": self.age_group,
            "occupation": self.occupation,
            "city_category": self.city_category,
            "stay_in_current_city_years": self.stay_in_current_city_years,
            "marital_status": self.marital_status,
            "product_name": self.product_name,
            "product_category": self.product_category,
            "supplier_id": self.supplier_id,
            "supplier_name": self.supplier_name,
            "store_name": self.store_name,
            "store_city": self.store_city,
            "revenue": self.revenue,
            "customer_enriched": self.customer_enriched,
            "product_enriched": self.product_enriched,
        }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Thread-safe Stream Buffer
# ─────────────────────────────────────────────────────────────────────────────

class StreamBuffer:
    """
    Thread-safe bounded stream buffer with capacity limit (default 1000).
    Supports 3 priority levels:
      - Priority 1: Online transactions (highest)
      - Priority 2: Periodic transactions
      - Priority 3: Shed-recovered transactions (lowest)
    When draining, higher-priority transactions are always extracted before lower-priority ones.
    """

    def __init__(self, capacity: int = STREAM_BUFFER_CAPACITY):
        self.capacity = capacity
        self._p1: collections.deque[Transaction] = collections.deque()
        self._p2: collections.deque[Transaction] = collections.deque()
        self._p3: collections.deque[Transaction] = collections.deque()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def _get_queue(self, priority: int) -> collections.deque[Transaction]:
        if priority == 1:
            return self._p1
        elif priority == 2:
            return self._p2
        else:
            return self._p3

    def put(
        self,
        item: Transaction,
        priority: int = 1,
        block: bool = True,
        timeout: Optional[float] = None,
    ) -> bool:
        """Put a single transaction into the buffer with given priority."""
        return bool(self.put_batch([item], priority=priority, block=block, timeout=timeout))

    def put_batch(
        self,
        items: List[Transaction],
        priority: int = 1,
        block: bool = True,
        timeout: Optional[float] = None,
    ) -> int:
        """
        Put a batch of transactions into the buffer with given priority (1, 2, or 3).
        If block is True, waits until there is space for at least 1 item or timeout expires.
        Returns number of items successfully placed into the buffer.
        """
        if not items:
            return 0

        target_queue = self._get_queue(priority)

        with self._lock:
            current_total = len(self._p1) + len(self._p2) + len(self._p3)
            if block:
                while current_total >= self.capacity:
                    if not self._not_full.wait(timeout=timeout):
                        return 0
                    current_total = len(self._p1) + len(self._p2) + len(self._p3)

            available = max(0, self.capacity - current_total)
            to_add = items[:available]
            for item in to_add:
                target_queue.append(item)

            if to_add:
                self._not_empty.notify_all()
            return len(to_add)

    def get_batch(self, max_items: int) -> List[Transaction]:
        """
        Get up to max_items in priority order (P1 -> P2 -> P3) without blocking.
        Returns empty list if buffer is empty.
        """
        with self._lock:
            extracted: List[Transaction] = []
            while self._p1 and len(extracted) < max_items:
                extracted.append(self._p1.popleft())
            while self._p2 and len(extracted) < max_items:
                extracted.append(self._p2.popleft())
            while self._p3 and len(extracted) < max_items:
                extracted.append(self._p3.popleft())

            if extracted:
                self._not_full.notify_all()
            return extracted

    def size(self) -> int:
        """Current total number of items in the buffer across all priorities."""
        with self._lock:
            return len(self._p1) + len(self._p2) + len(self._p3)

    def is_full(self) -> bool:
        """True if total items reached or exceeded capacity."""
        with self._lock:
            return (len(self._p1) + len(self._p2) + len(self._p3)) >= self.capacity

    def is_empty(self) -> bool:
        """True if buffer has 0 items across all priorities."""
        with self._lock:
            return (len(self._p1) + len(self._p2) + len(self._p3)) == 0

    def clear(self) -> None:
        """Clear all items from the buffer."""
        with self._lock:
            self._p1.clear()
            self._p2.clear()
            self._p3.clear()
            self._not_full.notify_all()


# ─────────────────────────────────────────────────────────────────────────────
# 3. Chunk
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Chunk:
    chunk_id: int
    transactions: List[Transaction]
    age: int = 0  # Number of iterations this chunk has been in memory


# ─────────────────────────────────────────────────────────────────────────────
# 4. MESHJOIN Engine
# ─────────────────────────────────────────────────────────────────────────────

class MeshJoinEngine:
    """
    MESHJOIN Engine with interleaved dual-pass enrichment.

    Algorithm per iteration:
    1. Read customer partition c_idx (500 tuples) into customer disk buffer.
    2. Read product partition p_idx (500 tuples) into product disk buffer.
    3. Read up to stream_chunk_size (500) transactions from StreamBuffer into H.
       Create a chunk reference and append to queue.
    4. Probe H_customer with customer disk buffer.
    5. Probe H_product with product disk buffer.
    6. Increment age of all chunks in queue.
    7. Evict chunks that have aged >= max_iterations (max(P_c, P_p) = 9 iterations).
       Remove their transactions from both H_customer and H_product.
       Output fully enriched transactions.
    8. Advance partition indices:
       c_idx = (c_idx + 1) % num_customer_partitions
       p_idx = (p_idx + 1) % num_product_partitions
    """

    def __init__(
        self,
        stream_buffer: StreamBuffer,
        customer_loader: Callable[[int], List[Any]],
        product_loader: Callable[[int], List[Any]],
        num_customer_partitions: int = 9,
        num_product_partitions: int = 8,
        partition_size: int = PARTITION_SIZE,
        stream_chunk_size: int = STREAM_CHUNK_SIZE,
    ):
        self.stream_buffer = stream_buffer
        self.customer_loader = customer_loader
        self.product_loader = product_loader
        self.num_customer_partitions = num_customer_partitions
        self.num_product_partitions = num_product_partitions
        self.partition_size = partition_size
        self.stream_chunk_size = stream_chunk_size

        # Chunks must see all partitions of both tables: max(9, 8) = 9 iterations
        self.max_iterations = max(num_customer_partitions, num_product_partitions)

        # Multi-map hash tables: ID -> list of Transaction references
        self.H_customer: Dict[str, List[Transaction]] = collections.defaultdict(list)
        self.H_product: Dict[str, List[Transaction]] = collections.defaultdict(list)

        # Queue of Chunk references
        self.queue: collections.deque[Chunk] = collections.deque()

        # State tracking
        self.c_idx: int = 0
        self.p_idx: int = 0
        self.iteration_count: int = 0
        self._next_chunk_id: int = 1
        self.total_input_count: int = 0
        self.total_output_count: int = 0

    def step(self) -> List[Transaction]:
        """
        Execute one complete MESHJOIN iteration.
        Returns: list of fully enriched transactions evicted during this iteration.
        """
        # 1. Load customer partition c_idx
        cust_partition = self.customer_loader(self.c_idx)

        # 2. Load product partition p_idx
        prod_partition = self.product_loader(self.p_idx)

        # 3. Read up to stream_chunk_size from stream buffer
        batch = self.stream_buffer.get_batch(self.stream_chunk_size)
        if batch:
            self.total_input_count += len(batch)
            for txn in batch:
                self.H_customer[txn.customer_id].append(txn)
                self.H_product[txn.product_id].append(txn)
            chunk = Chunk(chunk_id=self._next_chunk_id, transactions=batch, age=0)
            self._next_chunk_id += 1
            self.queue.append(chunk)

        # 4. Probe customer partition
        for row in cust_partition:
            cid = str(row["customer_id"] if isinstance(row, dict) else row[0])
            if cid in self.H_customer:
                for txn in self.H_customer[cid]:
                    if isinstance(row, dict):
                        txn.gender = row.get("gender")
                        txn.age_group = row.get("age_group")
                        txn.occupation = row.get("occupation")
                        txn.city_category = row.get("city_category")
                        txn.stay_in_current_city_years = row.get("stay_in_current_city_years")
                        txn.marital_status = row.get("marital_status")
                    else:
                        txn.gender = row[1]
                        txn.age_group = row[2]
                        txn.occupation = row[3]
                        txn.city_category = row[4]
                        txn.stay_in_current_city_years = row[5]
                        txn.marital_status = row[6]
                    txn.customer_enriched = True

        # 5. Probe product partition
        for row in prod_partition:
            pid = str(row["product_id"] if isinstance(row, dict) else row[0])
            if pid in self.H_product:
                for txn in self.H_product[pid]:
                    if isinstance(row, dict):
                        txn.product_name = row.get("product_name")
                        txn.product_category = row.get("product_category")
                        txn.supplier_id = row.get("supplier_id")
                        txn.supplier_name = row.get("supplier_name")
                    else:
                        txn.product_name = row[1]
                        txn.product_category = row[2]
                        txn.supplier_id = row[3]
                        txn.supplier_name = row[4]
                    txn.product_enriched = True

        # 6. Increment age of all chunks in queue
        for chunk in self.queue:
            chunk.age += 1

        # 7. Evict chunks that reached max_iterations
        evicted: List[Transaction] = []
        while self.queue and self.queue[0].age >= self.max_iterations:
            oldest_chunk = self.queue.popleft()
            for txn in oldest_chunk.transactions:
                cid = txn.customer_id
                if cid in self.H_customer:
                    self.H_customer[cid] = [t for t in self.H_customer[cid] if t is not txn]
                    if not self.H_customer[cid]:
                        del self.H_customer[cid]

                pid = txn.product_id
                if pid in self.H_product:
                    self.H_product[pid] = [t for t in self.H_product[pid] if t is not txn]
                    if not self.H_product[pid]:
                        del self.H_product[pid]

                evicted.append(txn)

        # 8. Advance partition pointers
        self.c_idx = (self.c_idx + 1) % self.num_customer_partitions
        self.p_idx = (self.p_idx + 1) % self.num_product_partitions
        self.iteration_count += 1
        self.total_output_count += len(evicted)

        return evicted

    def is_finished(self) -> bool:
        """True when StreamBuffer is empty and all chunks in queue have been evicted."""
        return self.stream_buffer.is_empty() and len(self.queue) == 0

    def run_all(self, callback: Optional[Callable[[List[Transaction]], None]] = None) -> List[Transaction]:
        """
        Run MESHJOIN iterations until is_finished() is True.
        Pass callback to receive batches of evicted transactions as they are produced.
        """
        results: List[Transaction] = []
        while not self.is_finished():
            evicted = self.step()
            if evicted:
                if callback:
                    callback(evicted)
                else:
                    results.extend(evicted)
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Return engine operational metrics."""
        return {
            "iterations": self.iteration_count,
            "c_idx": self.c_idx,
            "p_idx": self.p_idx,
            "queue_chunks": len(self.queue),
            "h_customer_keys": len(self.H_customer),
            "h_product_keys": len(self.H_product),
            "total_input": self.total_input_count,
            "total_output": self.total_output_count,
            "buffer_size": self.stream_buffer.size(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# 5. Database Partition Loader Factory
# ─────────────────────────────────────────────────────────────────────────────

def create_db_loaders(conn, partition_size: int = PARTITION_SIZE):
    """
    Creates partition loader functions that read 500-tuple partitions directly
    from MySQL customer_master_data and product_master_data tables.
    Never loads entire master data tables into memory.
    """
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT COUNT(*) AS cnt FROM customer_master_data")
    n_cust = cur.fetchone()["cnt"]
    num_cust_parts = math.ceil(n_cust / partition_size) if n_cust > 0 else 1

    cur.execute("SELECT COUNT(*) AS cnt FROM product_master_data")
    n_prod = cur.fetchone()["cnt"]
    num_prod_parts = math.ceil(n_prod / partition_size) if n_prod > 0 else 1
    cur.close()

    def customer_loader(part_idx: int) -> List[Dict[str, Any]]:
        c = conn.cursor(dictionary=True)
        offset = part_idx * partition_size
        c.execute(
            """
            SELECT customer_id, gender, age_group, occupation, city_category,
                   stay_in_current_city_years, marital_status
            FROM customer_master_data
            ORDER BY customer_id
            LIMIT %s OFFSET %s
            """,
            (partition_size, offset),
        )
        rows = c.fetchall()
        c.close()
        return rows

    def product_loader(part_idx: int) -> List[Dict[str, Any]]:
        c = conn.cursor(dictionary=True)
        offset = part_idx * partition_size
        c.execute(
            """
            SELECT product_id, product_name, product_category, supplier_id, supplier_name
            FROM product_master_data
            ORDER BY product_id
            LIMIT %s OFFSET %s
            """,
            (partition_size, offset),
        )
        rows = c.fetchall()
        c.close()
        return rows

    return customer_loader, product_loader, num_cust_parts, num_prod_parts
