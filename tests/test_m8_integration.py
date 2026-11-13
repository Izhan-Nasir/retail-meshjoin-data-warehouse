"""
tests/test_m8_integration.py
Module 8 unit tests — fast synthetic tests for consolidation, temporal derivation,
and runtime integration. No slow Excel loading.
"""

import datetime
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from consolidation import DataConsolidator, derive_time_attributes, get_season
from meshjoin import Transaction


# ─────────────────────────────────────────────────────────────────────────────
# Temporal Derivation Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_get_season():
    """Seasons match the standard retail calendar."""
    assert get_season(3) == "Spring"
    assert get_season(4) == "Spring"
    assert get_season(5) == "Spring"
    assert get_season(6) == "Summer"
    assert get_season(7) == "Summer"
    assert get_season(8) == "Summer"
    assert get_season(9) == "Fall"
    assert get_season(10) == "Fall"
    assert get_season(11) == "Fall"
    assert get_season(12) == "Winter"
    assert get_season(1) == "Winter"
    assert get_season(2) == "Winter"


def test_derive_time_attributes_spring_weekday():
    """Verify detailed attributes for a Wednesday in April."""
    t = derive_time_attributes("2011-04-13 14:30:00")
    assert str(t["event_date"]) == "2011-04-13"
    assert t["event_hour"] == 14
    assert t["day_of_month"] == 13
    assert t["month"] == 4
    assert t["quarter"] == 2
    assert t["year"] == 2011
    assert t["day_of_week"] == "Wednesday"
    assert t["is_weekend"] is False
    assert t["season"] == "Spring"
    assert t["half_year"] == "H1"


def test_derive_time_attributes_summer_weekend():
    """Verify detailed attributes for a Saturday in July."""
    t = derive_time_attributes("2011-07-16 09:15:00")
    assert str(t["event_date"]) == "2011-07-16"
    assert t["event_hour"] == 9
    assert t["day_of_month"] == 16
    assert t["month"] == 7
    assert t["quarter"] == 3
    assert t["year"] == 2011
    assert t["day_of_week"] == "Saturday"
    assert t["is_weekend"] is True
    assert t["season"] == "Summer"
    assert t["half_year"] == "H2"


def test_derive_time_attributes_winter_december():
    """Verify detailed attributes for December."""
    t = derive_time_attributes("2010-12-01 08:26:00")
    assert t["month"] == 12
    assert t["quarter"] == 4
    assert t["year"] == 2010
    assert t["season"] == "Winter"
    assert t["half_year"] == "H2"


# ─────────────────────────────────────────────────────────────────────────────
# Consolidation Logic Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_revenue_calculation_precision():
    """Revenue is accurately computed and rounded to 2 decimal places."""
    t = Transaction(
        transaction_id="T1",
        customer_id="101",
        product_id="P1",
        quantity=6,
        unit_price=2.55,
        event_time="2011-01-04 10:00:00",
        sales_mode="online",
    )
    assert t.revenue == 15.30


def test_processing_mode_equals_sales_mode():
    """processing_mode is guaranteed identical to sales_mode per human decision #1."""
    t_online = Transaction(
        transaction_id="1",
        customer_id="1",
        product_id="1",
        quantity=1,
        unit_price=1.0,
        event_time="2011-01-04 10:00:00",
        sales_mode="online",
    )
    assert t_online.processing_mode == "online"
    assert t_online.sales_mode == "online"

    t_periodic = Transaction(
        transaction_id="2",
        customer_id="2",
        product_id="2",
        quantity=1,
        unit_price=1.0,
        event_time="2011-01-04 10:00:00",
        sales_mode="periodic",
    )
    assert t_periodic.processing_mode == "periodic"
    assert t_periodic.sales_mode == "periodic"


def test_consolidator_in_memory_caching():
    """DataConsolidator caches prevent redundant queries."""
    consolidator = DataConsolidator(conn=None)
    # Pre-populate caches
    consolidator.customer_cache["101"] = 1
    consolidator.product_cache["P01"] = 2
    consolidator.supplier_cache["SUP01"] = 3
    consolidator.store_cache[1] = 4
    consolidator.time_cache[("2011-01-04", 10)] = 5

    txn = Transaction(
        transaction_id="T1",
        customer_id="101",
        product_id="P01",
        quantity=2,
        unit_price=5.0,
        event_time="2011-01-04 10:00:00",
        sales_mode="online",
        store_id=1,
        supplier_id="SUP01",
    )

    # get_or_create methods should return cached keys without calling cur
    assert consolidator.get_or_create_customer(txn, cur=None) == 1
    assert consolidator.get_or_create_product(txn, cur=None) == 2
    assert consolidator.get_or_create_supplier(txn, cur=None) == 3
    assert consolidator.get_or_create_store(txn, cur=None) == 4
    assert consolidator.get_or_create_time(txn.event_time, cur=None) == 5
