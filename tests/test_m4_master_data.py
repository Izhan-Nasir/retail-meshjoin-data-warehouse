"""
tests/test_m4_master_data.py
Module 4 test suite — 20 tests (10 unit + 10 integration)
Unit tests: synthetic mini-dataframe, no Excel required.
Integration tests: real cleaned dataset, require MySQL (password via env var MYSQL_PWD).
"""

import hashlib
import math
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from master_data import (
    stable,
    build_customer_md,
    build_product_md,
    GENDERS, AGE_GROUPS, OCCUPATIONS, CITY_CATS,
    STAY_YEARS, MARITAL, PRODUCT_CATS, SUPPLIERS,
    PARTITION_SIZE,
    write_csv,
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _stable_ref(text, n):
    h = hashlib.md5(str(text).encode("utf-8")).hexdigest()[:8]
    return int(h, 16) % n


def make_mini_df():
    """Minimal cleaned-shaped DataFrame for unit tests."""
    return pd.DataFrame({
        "customer_id": [12345.0, 12345.0, 67890.0],
        "product_id":  ["85123A", "85123A", "POST"],
        "description": ["WHITE HANGING HEART", "WHITE HANGING HEART T-LIGHT", "POSTAGE"],
        "country":     ["United Kingdom", "United Kingdom", "Germany"],
        "quantity":    [6, 6, 1],
        "unit_price":  [2.55, 2.55, 18.0],
        "event_time":  pd.to_datetime(["2011-01-04 10:00", "2011-01-04 10:00",
                                       "2011-01-04 11:00"]),
        "transaction_id": ["536365", "536365", "536366"],
        "revenue":     [15.30, 15.30, 18.0],
    })


# ─────────────────────────────────────────────────────────────────────────────
# UNIT TESTS (synthetic data, no Excel)
# ─────────────────────────────────────────────────────────────────────────────

def test_stable_deterministic():
    """stable() must return same value on repeated calls."""
    assert stable("12345", 7) == _stable_ref("12345", 7)
    assert stable("12345age", 7) == _stable_ref("12345age", 7)


def test_stable_range():
    """stable() result is always in [0, n)."""
    for text in ["abc", "12345", "POST", ""]:
        for n in [2, 3, 5, 7, 10]:
            assert 0 <= stable(text, n) < n


def test_customer_md_unique_ids():
    """build_customer_md returns exactly one row per unique customer_id."""
    df = make_mini_df()
    cust = build_customer_md(df)
    ids = [c["customer_id"] for c in cust]
    assert len(ids) == len(set(ids))
    assert set(ids) == {"12345", "67890"}


def test_customer_md_no_decimal():
    """customer_id must be a plain int string — no .0 suffix."""
    df = make_mini_df()
    cust = build_customer_md(df)
    for c in cust:
        assert "." not in c["customer_id"]
        int(c["customer_id"])   # must be parseable as int


def test_customer_md_valid_lookups():
    """Every customer attribute must be drawn from its legal set."""
    df = make_mini_df()
    cust = build_customer_md(df)
    for c in cust:
        assert c["gender"] in GENDERS
        assert c["age_group"] in AGE_GROUPS
        assert c["occupation"] in OCCUPATIONS
        assert c["city_category"] in CITY_CATS
        assert c["stay_in_current_city_years"] in STAY_YEARS
        assert c["marital_status"] in MARITAL


def test_customer_md_country_first_occurrence():
    """city_category uses FIRST occurrence country for multi-country customers."""
    # Build df where customer 12345 appears with UK first, then Germany
    df = pd.DataFrame({
        "customer_id": [12345.0, 12345.0],
        "product_id":  ["A", "B"],
        "description": ["x", "y"],
        "country":     ["United Kingdom", "Germany"],
        "quantity":    [1, 1],
        "unit_price":  [1.0, 1.0],
        "event_time":  pd.to_datetime(["2011-01-04 10:00", "2011-01-04 11:00"]),
        "transaction_id": ["1", "2"],
        "revenue":     [1.0, 1.0],
    })
    cust = build_customer_md(df)
    assert len(cust) == 1
    expected_city = CITY_CATS[stable("12345" + "United Kingdom", 3)]
    assert cust[0]["city_category"] == expected_city


def test_product_md_unique_ids():
    """build_product_md returns exactly one row per unique product_id."""
    df = make_mini_df()
    prods = build_product_md(df)
    ids = [p["product_id"] for p in prods]
    assert len(ids) == len(set(ids))
    assert set(ids) == {"85123A", "POST"}


def test_product_md_longest_description():
    """product_name must be the longest description per product."""
    df = make_mini_df()
    prods = build_product_md(df)
    p = next(p for p in prods if p["product_id"] == "85123A")
    # "WHITE HANGING HEART T-LIGHT" is longer than "WHITE HANGING HEART"
    assert p["product_name"] == "WHITE HANGING HEART T-LIGHT"


def test_product_md_valid_lookups():
    """Every product attribute must be drawn from its legal set."""
    df = make_mini_df()
    prods = build_product_md(df)
    sup_ids   = {s[0] for s in SUPPLIERS}
    sup_names = {s[1] for s in SUPPLIERS}
    for p in prods:
        assert p["product_category"] in PRODUCT_CATS
        assert p["supplier_id"] in sup_ids
        assert p["supplier_name"] in sup_names


def test_product_md_alphanumeric_preserved():
    """product_id must be preserved as-is (alphanumeric, no int conversion)."""
    df = make_mini_df()
    prods = build_product_md(df)
    ids = {p["product_id"] for p in prods}
    assert "85123A" in ids   # mixed alphanumeric
    assert "POST" in ids     # special code


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION TESTS (real dataset + MySQL)
# ─────────────────────────────────────────────────────────────────────────────

MYSQL_PWD = os.environ.get("MYSQL_PWD", "8956")

@pytest.fixture(scope="module")
def real_data():
    """Load the real cleaned dataset once for all integration tests."""
    from cleaning import load_and_clean
    from config import DATASET_PATH
    return load_and_clean(DATASET_PATH)


@pytest.fixture(scope="module")
def real_customers(real_data):
    return build_customer_md(real_data)


@pytest.fixture(scope="module")
def real_products(real_data):
    return build_product_md(real_data)


def test_real_customer_count(real_customers):
    """Must produce exactly 4,338 customer rows."""
    assert len(real_customers) == 4338


def test_real_customer_partitions(real_customers):
    """ceil(4338/500) == 9 partitions."""
    assert math.ceil(len(real_customers) / PARTITION_SIZE) == 9


def test_real_product_count(real_products):
    """Must produce exactly 3,665 product rows."""
    assert len(real_products) == 3665


def test_real_product_partitions(real_products):
    """ceil(3665/500) == 8 partitions."""
    assert math.ceil(len(real_products) / PARTITION_SIZE) == 8


def test_real_customer_csv_written(real_data, tmp_path):
    """write_csv produces a CSV with header + correct row count."""
    customers = build_customer_md(real_data)
    path = str(tmp_path / "cust.csv")
    write_csv(customers, path)
    df = pd.read_csv(path)
    assert len(df) == 4338
    assert "customer_id" in df.columns


def test_real_product_csv_written(real_data, tmp_path):
    """write_csv produces a CSV with header + correct row count."""
    products = build_product_md(real_data)
    path = str(tmp_path / "prod.csv")
    write_csv(products, path)
    df = pd.read_csv(path)
    assert len(df) == 3665
    assert "product_id" in df.columns


def test_real_customer_no_decimal(real_customers):
    """No customer_id may contain a decimal point."""
    bad = [c["customer_id"] for c in real_customers if "." in c["customer_id"]]
    assert bad == []


def test_real_product_name_no_empty(real_products):
    """No product_name may be blank (falls back to UNKNOWN)."""
    bad = [p["product_id"] for p in real_products if not p["product_name"].strip()]
    assert bad == []


def test_real_db_customer_load(real_customers):
    """customer_master_data table receives correct row count."""
    import mysql.connector
    from config import get_connection
    from master_data import load_customer_md_to_db
    conn = get_connection("localhost", 3306, "retail_dw", "root", MYSQL_PWD)
    load_customer_md_to_db(real_customers, conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM customer_master_data")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    assert count == 4338


def test_real_db_product_load(real_products):
    """product_master_data table receives correct row count."""
    import mysql.connector
    from config import get_connection
    from master_data import load_product_md_to_db
    conn = get_connection("localhost", 3306, "retail_dw", "root", MYSQL_PWD)
    load_product_md_to_db(real_products, conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM product_master_data")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    assert count == 3665
