"""
threads.py — OnlineThread, PeriodicThread, and MeshJoinThread.

Three threads cooperate via the thread-safe StreamBuffer and shared events:
1. OnlineThread: Streams online transactions at a dynamically configurable rate (peak 80/s, non-peak 10/s).
   Stops at end of file. Priority 1.
2. PeriodicThread: Extracts 1 group per simulated hour (60s), marks released, applies load shedding
   if buffer >= 800. Priority 2.
3. MeshJoinThread: Runs MESHJOIN iterations, flushes enriched transactions to callback, and recovers
   shed transactions in non-peak mode when buffer < 500. Priority 3 for recovered data.
"""

from __future__ import annotations

import csv
import datetime
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from config import (
    NON_PEAK_RATE,
    NUM_PERIODIC_GROUPS,
    ONLINE_CSV,
    PEAK_RATE,
    RECOVERY_THRESHOLD,
    SHED_THRESHOLD,
    SIMULATED_HOUR_SECONDS,
    STREAM_BUFFER_CAPACITY,
)
from meshjoin import MeshJoinEngine, StreamBuffer, Transaction
from shedding import recover_shed_transactions, shed_transactions, should_recover, should_shed


# ─────────────────────────────────────────────────────────────────────────────
# 1. OnlineThread
# ─────────────────────────────────────────────────────────────────────────────

class OnlineThread(threading.Thread):
    """
    Reads online transactions from CSV and pushes them into StreamBuffer at Priority 1.
    Rate (tx/s) can be dynamically adjusted without restarting the thread.
    Stops when CSV reaches EOF (no replay, per decision Q9).
    """

    def __init__(
        self,
        stream_buffer: StreamBuffer,
        csv_path: str = ONLINE_CSV,
        rate: int = PEAK_RATE,
        enabled_event: Optional[threading.Event] = None,
        stop_event: Optional[threading.Event] = None,
    ):
        super().__init__(name="OnlineThread", daemon=True)
        self.stream_buffer = stream_buffer
        self.csv_path = csv_path
        self.rate = rate
        self.enabled_event = enabled_event or threading.Event()
        if enabled_event is None:
            self.enabled_event.set()  # Enabled by default

        self.stop_event = stop_event or threading.Event()

        self.tx_sent: int = 0
        self.is_eof: bool = False
        self._lock = threading.Lock()

    def set_rate(self, new_rate: int) -> None:
        """Dynamically update stream emission rate (tx/sec)."""
        with self._lock:
            self.rate = max(1, int(new_rate))

    def get_rate(self) -> int:
        with self._lock:
            return self.rate

    def run(self) -> None:
        try:
            with open(self.csv_path, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if self.stop_event.is_set():
                        break

                    # Wait while thread is paused
                    while not self.enabled_event.is_set() and not self.stop_event.is_set():
                        time.sleep(0.05)

                    if self.stop_event.is_set():
                        break

                    txn = Transaction(
                        transaction_id=row["transaction_id"],
                        customer_id=row["customer_id"],
                        product_id=row["product_id"],
                        quantity=int(row["quantity"]),
                        unit_price=float(row["unit_price"]),
                        event_time=row["event_time"],
                        sales_mode=row.get("sales_mode", "online"),
                        processing_mode=row.get("sales_mode", "online"),
                        store_id=1,
                    )

                    # Put into stream buffer at Priority 1 (blocks if buffer is full)
                    while not self.stop_event.is_set():
                        added = self.stream_buffer.put(txn, priority=1, block=True, timeout=0.2)
                        if added:
                            self.tx_sent += 1
                            break

                    # Rate limiting sleep
                    current_rate = self.get_rate()
                    if current_rate > 0:
                        time.sleep(1.0 / current_rate)

            self.is_eof = True
        except FileNotFoundError:
            print(f"[OnlineThread] File not found: {self.csv_path}")
            self.is_eof = True


# ─────────────────────────────────────────────────────────────────────────────
# 2. PeriodicThread
# ─────────────────────────────────────────────────────────────────────────────

class PeriodicThread(threading.Thread):
    """
    Extracts periodic transaction groups from periodic_sales_data or a custom loader.
    Fires once per simulated hour (simulated_hour_seconds).
    Sheds transactions to shed_table if buffer >= 800.
    Priority 2.
    """

    def __init__(
        self,
        stream_buffer: StreamBuffer,
        group_loader: Optional[Callable[[], Optional[List[Transaction]]]] = None,
        conn=None,
        simulated_hour_seconds: float = SIMULATED_HOUR_SECONDS,
        enabled_event: Optional[threading.Event] = None,
        stop_event: Optional[threading.Event] = None,
        shed_sink: Optional[List[Transaction]] = None,
    ):
        super().__init__(name="PeriodicThread", daemon=True)
        self.stream_buffer = stream_buffer
        self.group_loader = group_loader
        self.conn = conn
        self.simulated_hour_seconds = simulated_hour_seconds
        self.enabled_event = enabled_event or threading.Event()
        if enabled_event is None:
            self.enabled_event.set()

        self.stop_event = stop_event or threading.Event()
        self.shed_sink = shed_sink

        self.groups_released: int = 0
        self.total_tx_sent: int = 0
        self.total_tx_shed: int = 0
        self.is_finished: bool = False

    def _default_db_group_loader(self) -> Optional[List[Transaction]]:
        """Extract the next unreleased group from MySQL periodic_sales_data."""
        if not self.conn:
            return None

        cur = self.conn.cursor(dictionary=True)
        # Find the minimum unreleased hour
        cur.execute("SELECT MIN(release_hour) AS next_hr FROM periodic_sales_data WHERE released = FALSE")
        row = cur.fetchone()
        if not row or row["next_hr"] is None:
            cur.close()
            return None

        next_hr = row["next_hr"]
        cur.execute(
            """
            SELECT transaction_id, customer_id, product_id, quantity, unit_price,
                   event_time, sales_mode, processing_mode, store_id
            FROM periodic_sales_data
            WHERE release_hour = %s AND released = FALSE
            ORDER BY id ASC
            """,
            (next_hr,),
        )
        rows = cur.fetchall()

        # Mark released
        cur.execute(
            "UPDATE periodic_sales_data SET released = TRUE WHERE release_hour = %s",
            (next_hr,),
        )
        self.conn.commit()
        cur.close()

        return [
            Transaction(
                transaction_id=str(r["transaction_id"]),
                customer_id=str(r["customer_id"]),
                product_id=str(r["product_id"]),
                quantity=int(r["quantity"]),
                unit_price=float(r["unit_price"]),
                event_time=r["event_time"],
                sales_mode=r["sales_mode"],
                processing_mode=r["processing_mode"],
                store_id=int(r["store_id"]),
            )
            for r in rows
        ]

    def run(self) -> None:
        loader = self.group_loader or self._default_db_group_loader

        while not self.stop_event.is_set():
            # Wait while thread is paused
            while not self.enabled_event.is_set() and not self.stop_event.is_set():
                time.sleep(0.05)

            if self.stop_event.is_set():
                break

            # Extract next group
            group = loader()
            if group is None or len(group) == 0:
                self.is_finished = True
                break

            self.groups_released += 1
            current_buf_size = self.stream_buffer.size()

            if should_shed(current_buf_size, incoming_is_periodic=True):
                # Load shedding triggered
                available_space = max(0, STREAM_BUFFER_CAPACITY - current_buf_size)
                if available_space > 0:
                    to_load = group[:available_space]
                    to_shed = group[available_space:]
                    self.stream_buffer.put_batch(to_load, priority=2, block=False)
                    self.total_tx_sent += len(to_load)
                    if to_shed:
                        shed_count = shed_transactions(to_shed, conn=self.conn, in_memory_sink=self.shed_sink)
                        self.total_tx_shed += shed_count
                else:
                    shed_count = shed_transactions(group, conn=self.conn, in_memory_sink=self.shed_sink)
                    self.total_tx_shed += shed_count
            else:
                # Normal loading
                self.stream_buffer.put_batch(group, priority=2, block=True, timeout=2.0)
                self.total_tx_sent += len(group)

            # Wait for next simulated hour
            wait_step = 0.1
            slept = 0.0
            while slept < self.simulated_hour_seconds and not self.stop_event.is_set():
                time.sleep(wait_step)
                slept += wait_step

        self.is_finished = True


# ─────────────────────────────────────────────────────────────────────────────
# 3. MeshJoinThread
# ─────────────────────────────────────────────────────────────────────────────

class MeshJoinThread(threading.Thread):
    """
    Executes MESHJOIN iterations continuously.
    Coordinates shed recovery in non-peak mode when buffer < 500.
    Emits enriched transactions to an output callback.
    """

    def __init__(
        self,
        engine: MeshJoinEngine,
        output_callback: Optional[Callable[[List[Transaction]], None]] = None,
        conn=None,
        is_peak_func: Optional[Callable[[], bool]] = None,
        stop_event: Optional[threading.Event] = None,
        shed_sink: Optional[List[Transaction]] = None,
    ):
        super().__init__(name="MeshJoinThread", daemon=True)
        self.engine = engine
        self.output_callback = output_callback
        self.conn = conn
        self.is_peak_func = is_peak_func or (lambda: True)
        self.stop_event = stop_event or threading.Event()
        self.shed_sink = shed_sink

        self.total_recovered: int = 0
        self.total_enriched: int = 0

    def run(self) -> None:
        while not self.stop_event.is_set():
            # Check for shed recovery opportunity
            buf_size = self.engine.stream_buffer.size()
            is_peak = self.is_peak_func()

            if should_recover(buf_size, is_peak=is_peak):
                capacity_available = RECOVERY_THRESHOLD - buf_size
                recovered = recover_shed_transactions(
                    max_count=capacity_available,
                    conn=self.conn,
                    in_memory_sink=self.shed_sink,
                )
                if recovered:
                    self.engine.stream_buffer.put_batch(recovered, priority=3, block=False)
                    self.total_recovered += len(recovered)

            # Execute MESHJOIN iteration
            evicted = self.engine.step()
            if evicted:
                self.total_enriched += len(evicted)
                if self.output_callback:
                    self.output_callback(evicted)

            # Idle backoff if no work available
            if self.engine.stream_buffer.is_empty() and len(self.engine.queue) == 0:
                time.sleep(0.01)
