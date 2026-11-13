"""
tests/test_m5_data_prep.py
Module 5 unit tests — fast synthetic data only, no Excel loading.
"""

import hashlib
import math
import os
import sys
import io
import csv

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data_prep import (
    assign_sales_mode,
    assign_store_id,
    prepare_data,
    write_online_csv,
    stage_periodic,
    ONLINE_COLUMNS,
    NUM_PERIODIC_GROUPS,
)
from config import NUM_PERIODIC_GROUPS as CFG_GROUPS


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _md5_pct(text):
    return int(hashlib.md5(str(text).encode()).hexdigest()[:8], 16) % 10


def make_df(n_invoices=20):
    """Synthetic cleaned DataFrame with n_invoices unique invoices."""
    rows = []
    for i in range(n_invoices):
        inv = 500000 + i
        rows.append({
            "transaction_id": str(inv),
            "customer_id":    12345.0 + i,
            "product_id":     f"PROD{i:03d}",
            "description":    f"Product {i}",
            "quantity":       2,
            "unit_price":     3.50,
            "event_time":     pd.Timestamp("2011-01-04 10:00:00") + pd.Timedelta(hours=i),
            "country":        "United Kingdom",
            "revenue":        7.0,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_assign_sales_mode_values():
    """assign_sales_mode must return only 'online' or 'periodic'."""
    for inv in [536365, 536366, 999999, 100001]:
        mode = assign_sales_mode(inv)
        assert mode in ("online", "periodic")


def test_assign_sales_mode_hash_rule():
    """online when hash%10 <= 6, periodic when hash%10 >= 7."""
    for inv in range(536300, 536400):
        h = _md5_pct(str(int(inv)))
        expected = "online" if h <= 6 else "periodic"
        assert assign_sales_mode(inv) == expected


def test_assign_store_id_online():
    """Online transactions always get store_id = 1."""
    for txn in [536365, 999999, 1]:
        assert assign_store_id("online", txn) == 1


def test_assign_store_id_periodic_range():
    """Periodic transactions get store_id in [2, 5]."""
    for txn in range(536300, 536400):
        sid = assign_store_id("periodic", txn)
        assert 2 <= sid <= 5


def test_prepare_data_splits():
    """prepare_data must split into online + periodic == total rows."""
    df = make_df(50)
    online_df, periodic_df = prepare_data(df)
    assert len(online_df) + len(periodic_df) == len(df)


def test_prepare_data_mode_columns():
    """sales_mode and processing_mode must exist and be equal."""
    df = make_df(20)
    online_df, periodic_df = prepare_data(df)
    combined = pd.concat([online_df, periodic_df])
    assert (combined["sales_mode"] == combined["processing_mode"]).all()


def test_prepare_data_online_store_id():
    """All online rows must have store_id == 1."""
    df = make_df(50)
    online_df, _ = prepare_data(df)
    assert (online_df["store_id"] == 1).all()


def test_prepare_data_periodic_store_id():
    """All periodic rows must have store_id in [2, 5]."""
    df = make_df(50)
    _, periodic_df = prepare_data(df)
    assert periodic_df["store_id"].between(2, 5).all()


def test_online_csv_columns(tmp_path):
    """Online CSV must contain exactly the required columns."""
    df = make_df(20)
    online_df, _ = prepare_data(df)
    path = str(tmp_path / "online.csv")
    write_online_csv(online_df, path)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    assert header == ONLINE_COLUMNS


def test_online_csv_row_count(tmp_path):
    """Online CSV row count (excl. header) must match online_df length."""
    df = make_df(20)
    online_df, _ = prepare_data(df)
    path = str(tmp_path / "online.csv")
    n = write_online_csv(online_df, path)
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) - 1 == n == len(online_df)


def test_periodic_group_count():
    """NUM_PERIODIC_GROUPS must be 48 (matches config)."""
    assert NUM_PERIODIC_GROUPS == 48
    assert CFG_GROUPS == 48


def test_invoice_level_mode_consistency():
    """All rows with the same invoice_no must share the same sales_mode."""
    # Build df where invoice 536365 appears twice
    df = pd.DataFrame({
        "transaction_id": ["536365", "536365", "536366"],
        "customer_id":    [12345.0, 12345.0, 67890.0],
        "product_id":     ["A", "B", "C"],
        "description":    ["x", "y", "z"],
        "quantity":       [1, 2, 3],
        "unit_price":     [1.0, 2.0, 3.0],
        "event_time":     pd.to_datetime(["2011-01-04 10:00"] * 3),
        "country":        ["UK"] * 3,
        "revenue":        [1.0, 4.0, 9.0],
    })
    online_df, periodic_df = prepare_data(df)
    combined = pd.concat([online_df, periodic_df])
    grp = combined.groupby("transaction_id")["sales_mode"].nunique()
    assert (grp == 1).all(), "Invoice rows have inconsistent sales_mode"
