"""
tests/test_m6_meshjoin.py
Module 6 unit tests — fast synthetic data fixtures only, no slow Excel loading.
"""

import collections
import datetime
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from meshjoin import (
    StreamBuffer,
    Transaction,
    Chunk,
    MeshJoinEngine,
)
from config import STORE_LIST, STREAM_BUFFER_CAPACITY


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures and Mock Data
# ─────────────────────────────────────────────────────────────────────────────

def make_test_txn(txn_id="1", cid="101", pid="P01", qty=2, price=5.0, mode="online", store_id=1):
    return Transaction(
        transaction_id=str(txn_id),
        customer_id=str(cid),
        product_id=str(pid),
        quantity=qty,
        unit_price=price,
        event_time="2011-01-04 10:00:00",
        sales_mode=mode,
        store_id=store_id,
    )


def make_mock_loaders():
    """
    Creates mock loaders for 3 customer partitions and 2 product partitions.
    """
    # 3 customer partitions (2 customers each = 6 total)
    cust_data = [
        [
            {"customer_id": "101", "gender": "Female", "age_group": "26-35", "occupation": "Engineer",
             "city_category": "A", "stay_in_current_city_years": "2", "marital_status": "Single"},
            {"customer_id": "102", "gender": "Male", "age_group": "36-45", "occupation": "Doctor",
             "city_category": "B", "stay_in_current_city_years": "3", "marital_status": "Married"},
        ],
        [
            {"customer_id": "103", "gender": "Male", "age_group": "18-25", "occupation": "Student",
             "city_category": "C", "stay_in_current_city_years": "1", "marital_status": "Single"},
            {"customer_id": "104", "gender": "Female", "age_group": "46-50", "occupation": "Teacher",
             "city_category": "A", "stay_in_current_city_years": "4+", "marital_status": "Married"},
        ],
        [
            {"customer_id": "105", "gender": "Female", "age_group": "51-55", "occupation": "Executive",
             "city_category": "B", "stay_in_current_city_years": "0", "marital_status": "Married"},
            {"customer_id": "106", "gender": "Male", "age_group": "55+", "occupation": "Retired",
             "city_category": "C", "stay_in_current_city_years": "4+", "marital_status": "Single"},
        ],
    ]

    # 2 product partitions (2 products each = 4 total)
    prod_data = [
        [
            {"product_id": "P01", "product_name": "Teapot", "product_category": "Kitchen",
             "supplier_id": "SUP01", "supplier_name": "Northwind Supplies"},
            {"product_id": "P02", "product_name": "Desk Lamp", "product_category": "Home Decor",
             "supplier_id": "SUP02", "supplier_name": "Atlantic Gifts Ltd"},
        ],
        [
            {"product_id": "P03", "product_name": "Notebook", "product_category": "Stationery",
             "supplier_id": "SUP03", "supplier_name": "Midlands Wholesale"},
            {"product_id": "P04", "product_name": "Teddy Bear", "product_category": "Toys",
             "supplier_id": "SUP04", "supplier_name": "Celtic Craft Co"},
        ],
    ]

    def cust_loader(idx):
        return cust_data[idx % len(cust_data)]

    def prod_loader(idx):
        return prod_data[idx % len(prod_data)]

    return cust_loader, prod_loader, len(cust_data), len(prod_data)


# ─────────────────────────────────────────────────────────────────────────────
# Stream Buffer Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_stream_buffer_capacity():
    """Stream buffer capacity limit is enforced."""
    buf = StreamBuffer(capacity=5)
    txns = [make_test_txn(i) for i in range(10)]
    added = buf.put_batch(txns, block=False)
    assert added == 5
    assert buf.size() == 5
    assert buf.is_full() is True


def test_stream_buffer_fifo():
    """Stream buffer get_batch retrieves items in strict FIFO order."""
    buf = StreamBuffer(capacity=10)
    txns = [make_test_txn(i) for i in range(5)]
    buf.put_batch(txns)
    out = buf.get_batch(3)
    assert len(out) == 3
    assert [t.transaction_id for t in out] == ["0", "1", "2"]
    remaining = buf.get_batch(10)
    assert len(remaining) == 2
    assert [t.transaction_id for t in remaining] == ["3", "4"]
    assert buf.is_empty() is True


# ─────────────────────────────────────────────────────────────────────────────
# Transaction & Enrichment Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_transaction_creation_and_store_enrichment():
    """Transaction initializes properly and enriches store info from STORE_LIST."""
    t1 = make_test_txn(1, store_id=1)
    assert t1.store_name == STORE_LIST[1]["store_name"]
    assert t1.store_city == STORE_LIST[1]["city"]

    t2 = make_test_txn(2, store_id=3)
    assert t2.store_name == STORE_LIST[3]["store_name"]
    assert t2.store_city == STORE_LIST[3]["city"]


def test_transaction_revenue_calc():
    """Transaction calculates revenue = quantity * unit_price rounded to 2 decimals."""
    t = make_test_txn(qty=3, price=1.99)
    assert t.revenue == 5.97


# ─────────────────────────────────────────────────────────────────────────────
# MESHJOIN Engine Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_hash_table_multi_map():
    """Multiple transactions sharing the same customer or product ID coexist in H."""
    buf = StreamBuffer()
    c_loader, p_loader, nc, np = make_mock_loaders()
    engine = MeshJoinEngine(buf, c_loader, p_loader, nc, np, stream_chunk_size=10)

    # 3 transactions with customer "101", 2 with product "P01"
    txns = [
        make_test_txn("T1", "101", "P01"),
        make_test_txn("T2", "101", "P02"),
        make_test_txn("T3", "101", "P01"),
    ]
    buf.put_batch(txns)
    engine.step()

    assert len(engine.H_customer["101"]) == 3
    assert len(engine.H_product["P01"]) == 2
    assert len(engine.H_product["P02"]) == 1


def test_queue_age_and_eviction():
    """A chunk is evicted after exactly max(P_c, P_p) iterations."""
    buf = StreamBuffer()
    c_loader, p_loader, nc, np = make_mock_loaders()  # nc=3, np=2 -> max=3 iterations
    engine = MeshJoinEngine(buf, c_loader, p_loader, nc, np, stream_chunk_size=10)

    txns = [make_test_txn("T1", "101", "P01")]
    buf.put_batch(txns)

    # Iteration 1: chunk enters, probed by C0 & P0, age becomes 1. No eviction.
    evicted_1 = engine.step()
    assert len(evicted_1) == 0
    assert len(engine.queue) == 1
    assert engine.queue[0].age == 1

    # Iteration 2: probed by C1 & P1, age becomes 2. No eviction.
    evicted_2 = engine.step()
    assert len(evicted_2) == 0
    assert engine.queue[0].age == 2

    # Iteration 3: probed by C2 & P0, age becomes 3 (>= max_iterations 3). Evicted!
    evicted_3 = engine.step()
    assert len(evicted_3) == 1
    assert evicted_3[0].transaction_id == "T1"
    assert len(engine.queue) == 0


def test_interleaved_dual_pass_enrichment():
    """After eviction, all customer and product attributes are populated."""
    buf = StreamBuffer()
    c_loader, p_loader, nc, np = make_mock_loaders()  # nc=3, np=2
    engine = MeshJoinEngine(buf, c_loader, p_loader, nc, np, stream_chunk_size=10)

    # T1 has customer 105 (in C2) and product P03 (in P1)
    txns = [make_test_txn("T1", "105", "P03")]
    buf.put_batch(txns)

    evicted = engine.run_all()
    assert len(evicted) == 1
    t = evicted[0]

    # Verify customer enrichment from C2
    assert t.gender == "Female"
    assert t.age_group == "51-55"
    assert t.occupation == "Executive"
    assert t.city_category == "B"
    assert t.marital_status == "Married"
    assert t.customer_enriched is True

    # Verify product enrichment from P1
    assert t.product_name == "Notebook"
    assert t.product_category == "Stationery"
    assert t.supplier_id == "SUP03"
    assert t.supplier_name == "Midlands Wholesale"
    assert t.product_enriched is True
    assert t.is_fully_enriched() is True


def test_cyclic_partition_scanning():
    """Partition indices wrap around correctly modulo partition counts."""
    buf = StreamBuffer()
    c_loader, p_loader, nc, np = make_mock_loaders()  # nc=3, np=2
    engine = MeshJoinEngine(buf, c_loader, p_loader, nc, np)

    assert engine.c_idx == 0 and engine.p_idx == 0
    engine.step()
    assert engine.c_idx == 1 and engine.p_idx == 1
    engine.step()
    assert engine.c_idx == 2 and engine.p_idx == 0  # product wrapped (2 % 2 = 0)
    engine.step()
    assert engine.c_idx == 0 and engine.p_idx == 1  # customer wrapped (3 % 3 = 0)


def test_hash_table_cleanup_on_eviction():
    """Evicted transactions are purged from H_customer and H_product (no memory leak)."""
    buf = StreamBuffer()
    c_loader, p_loader, nc, np = make_mock_loaders()
    engine = MeshJoinEngine(buf, c_loader, p_loader, nc, np)

    txns = [make_test_txn("T1", "101", "P01")]
    buf.put_batch(txns)

    engine.run_all()
    assert len(engine.H_customer) == 0
    assert len(engine.H_product) == 0
    assert len(engine.queue) == 0


def test_no_data_loss_or_duplication():
    """All transactions entered into the buffer are output exactly once."""
    buf = StreamBuffer()
    c_loader, p_loader, nc, np = make_mock_loaders()
    engine = MeshJoinEngine(buf, c_loader, p_loader, nc, np, stream_chunk_size=2)

    # 6 transactions across different customers and products
    in_txns = [
        make_test_txn("T1", "101", "P01"),
        make_test_txn("T2", "102", "P02"),
        make_test_txn("T3", "103", "P03"),
        make_test_txn("T4", "104", "P04"),
        make_test_txn("T5", "105", "P01"),
        make_test_txn("T6", "106", "P02"),
    ]
    buf.put_batch(in_txns)

    out_txns = engine.run_all()
    assert len(out_txns) == 6
    assert [t.transaction_id for t in out_txns] == ["T1", "T2", "T3", "T4", "T5", "T6"]
    for t in out_txns:
        assert t.is_fully_enriched() is True


def test_multi_transaction_same_customer_preserves_other():
    """Evicting a transaction in Chunk 1 does not evict another transaction in Chunk 2 sharing same customer."""
    buf = StreamBuffer()
    c_loader, p_loader, nc, np = make_mock_loaders()
    engine = MeshJoinEngine(buf, c_loader, p_loader, nc, np, stream_chunk_size=1)

    # Feed T1 in iteration 1
    buf.put(make_test_txn("T1", "101", "P01"))
    engine.step()

    # Feed T2 (same customer 101) in iteration 2
    buf.put(make_test_txn("T2", "101", "P02"))
    engine.step()

    # Iteration 3
    engine.step()

    # T1 evicts at iteration 3 (its age is 3)
    # T2 is still in queue (age is 2)
    assert "101" in engine.H_customer
    assert len(engine.H_customer["101"]) == 1
    assert engine.H_customer["101"][0].transaction_id == "T2"

    # Next iteration: T2 reaches age 3 and evicts
    engine.step()
    assert "101" not in engine.H_customer
