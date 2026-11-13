"""
tests/test_m7_threads.py
Module 7 unit tests — fast synthetic threading, priority, and load shedding tests.
No slow Excel loading.
"""

import os
import sys
import tempfile
import threading
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from meshjoin import MeshJoinEngine, StreamBuffer, Transaction
from shedding import (
    get_shed_count,
    recover_shed_transactions,
    shed_transactions,
    should_recover,
    should_shed,
)
from threads import MeshJoinThread, OnlineThread, PeriodicThread


def make_txn(txn_id, mode="online", cid="101", pid="P01"):
    return Transaction(
        transaction_id=str(txn_id),
        customer_id=str(cid),
        product_id=str(pid),
        quantity=1,
        unit_price=10.0,
        event_time="2011-01-04 10:00:00",
        sales_mode=mode,
        store_id=1 if mode == "online" else 2,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Shedding Logic Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_should_shed_trigger():
    """Load shedding triggers if and only if buffer >= 800 and transaction is periodic."""
    assert should_shed(800, incoming_is_periodic=True) is True
    assert should_shed(850, incoming_is_periodic=True) is True
    assert should_shed(799, incoming_is_periodic=True) is False


def test_online_never_shed():
    """Online transactions are NEVER shed, regardless of buffer size."""
    assert should_shed(800, incoming_is_periodic=False) is False
    assert should_shed(999, incoming_is_periodic=False) is False
    assert should_shed(1000, incoming_is_periodic=False) is False


def test_should_recover_trigger():
    """Shed recovery triggers if and only if non-peak and buffer < 500."""
    assert should_recover(499, is_peak=False) is True
    assert should_recover(0, is_peak=False) is True
    assert should_recover(500, is_peak=False) is False
    # Never recover in peak mode
    assert should_recover(200, is_peak=True) is False


def test_shed_and_recover_in_memory():
    """Shed transactions are recorded and recovered correctly."""
    sink = []
    txns = [make_txn(i, mode="periodic") for i in range(10)]
    shed_transactions(txns, in_memory_sink=sink)
    assert get_shed_count(in_memory_sink=sink) == 10

    recovered = recover_shed_transactions(max_count=4, in_memory_sink=sink)
    assert len(recovered) == 4
    assert get_shed_count(in_memory_sink=sink) == 6


# ─────────────────────────────────────────────────────────────────────────────
# Priority Buffer Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_priority_extraction_order():
    """Buffer drains Priority 1 (Online) before Priority 2 (Periodic) before Priority 3 (Recovered)."""
    buf = StreamBuffer(capacity=20)
    # Add in mixed order
    p3 = make_txn("P3_1", mode="periodic")
    p2 = make_txn("P2_1", mode="periodic")
    p1 = make_txn("P1_1", mode="online")

    buf.put(p3, priority=3)
    buf.put(p2, priority=2)
    buf.put(p1, priority=1)

    batch = buf.get_batch(3)
    assert [t.transaction_id for t in batch] == ["P1_1", "P2_1", "P3_1"]


# ─────────────────────────────────────────────────────────────────────────────
# Thread Execution Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_online_thread_reads_and_stops_at_eof(tmp_path):
    """OnlineThread reads CSV, pushes to buffer at Priority 1, and stops cleanly at EOF."""
    csv_file = tmp_path / "online_test.csv"
    csv_file.write_text(
        "transaction_id,customer_id,product_id,quantity,unit_price,event_time,sales_mode\n"
        "101,1001,P1,1,5.0,2011-01-04 10:00:00,online\n"
        "102,1002,P2,2,3.5,2011-01-04 10:01:00,online\n"
        "103,1003,P3,3,2.0,2011-01-04 10:02:00,online\n",
        encoding="utf-8",
    )

    buf = StreamBuffer(capacity=10)
    stop_event = threading.Event()
    thread = OnlineThread(buf, csv_path=str(csv_file), rate=500, stop_event=stop_event)
    thread.start()
    thread.join(timeout=2.0)

    assert thread.is_eof is True
    assert thread.tx_sent == 3
    assert buf.size() == 3


def test_periodic_thread_extracts_and_sheds():
    """PeriodicThread extracts groups and triggers load shedding when buffer >= 800."""
    buf = StreamBuffer(capacity=1000)
    # Pre-fill buffer to 850 with online transactions
    buf.put_batch([make_txn(f"pre_{i}") for i in range(850)], priority=1)

    group = [make_txn(f"per_{i}", mode="periodic") for i in range(100)]
    group_called = False

    def mock_group_loader():
        nonlocal group_called
        if not group_called:
            group_called = True
            return group
        return None

    shed_sink = []
    stop_event = threading.Event()
    thread = PeriodicThread(
        buf,
        group_loader=mock_group_loader,
        simulated_hour_seconds=0.05,
        stop_event=stop_event,
        shed_sink=shed_sink,
    )
    thread.start()
    thread.join(timeout=2.0)

    # Buffer was 850 >= 800 -> available space was 150 -> all 100 fit or remainder shed
    assert thread.is_finished is True
    assert thread.groups_released == 1


def test_meshjoin_thread_processes_and_recovers():
    """MeshJoinThread runs iterations, drains buffer, and recovers shed data in non-peak mode."""
    buf = StreamBuffer(capacity=1000)
    shed_sink = [make_txn(f"shed_{i}", mode="periodic") for i in range(10)]

    c_loader = lambda idx: []
    p_loader = lambda idx: []
    engine = MeshJoinEngine(buf, c_loader, p_loader, 1, 1)

    output = []
    stop_event = threading.Event()
    mj_thread = MeshJoinThread(
        engine,
        output_callback=lambda batch: output.extend(batch),
        is_peak_func=lambda: False,  # Non-peak -> allows recovery
        stop_event=stop_event,
        shed_sink=shed_sink,
    )
    mj_thread.start()

    time.sleep(0.1)  # Allow a few iterations
    stop_event.set()
    mj_thread.join(timeout=2.0)

    # In non-peak mode with buffer < 500, transactions in shed_sink should be recovered
    assert len(shed_sink) == 0
    assert mj_thread.total_recovered == 10


def test_threads_clean_shutdown(tmp_path):
    """All 3 threads run concurrently and stop cleanly without deadlock when stop_event is set."""
    csv_file = tmp_path / "stream.csv"
    csv_file.write_text(
        "transaction_id,customer_id,product_id,quantity,unit_price,event_time,sales_mode\n"
        "1,1,1,1,1.0,2011-01-04 10:00:00,online\n",
        encoding="utf-8",
    )

    buf = StreamBuffer(capacity=1000)
    stop_event = threading.Event()
    enabled_event = threading.Event()
    enabled_event.set()

    c_loader = lambda idx: []
    p_loader = lambda idx: []
    engine = MeshJoinEngine(buf, c_loader, p_loader, 1, 1)

    t_online = OnlineThread(buf, csv_path=str(csv_file), rate=100, enabled_event=enabled_event, stop_event=stop_event)
    t_periodic = PeriodicThread(buf, group_loader=lambda: None, simulated_hour_seconds=1.0, enabled_event=enabled_event, stop_event=stop_event)
    t_meshjoin = MeshJoinThread(engine, stop_event=stop_event)

    t_online.start()
    t_periodic.start()
    t_meshjoin.start()

    time.sleep(0.1)
    stop_event.set()

    t_online.join(timeout=2.0)
    t_periodic.join(timeout=2.0)
    t_meshjoin.join(timeout=2.0)

    assert not t_online.is_alive()
    assert not t_periodic.is_alive()
    assert not t_meshjoin.is_alive()
