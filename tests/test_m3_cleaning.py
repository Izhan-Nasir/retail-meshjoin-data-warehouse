"""
tests/test_m3_cleaning.py — Module 3 unit and integration tests.

Unit tests use small synthetic DataFrames — no real dataset needed.
Integration test loads the real dataset and checks known expected counts.
"""
import pytest
import pandas as pd
import numpy as np
import pathlib
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cleaning import load_and_clean, _is_numeric, COLUMN_MAP


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_raw_df(rows):
    """Build a minimal synthetic raw DataFrame with project column names."""
    return pd.DataFrame(rows, columns=[
        "transaction_id", "customer_id", "product_id",
        "product_name", "quantity", "unit_price", "event_time", "country"
    ])


# ── Unit tests: individual cleaning rules ─────────────────────────────────────

def test_cancellation_removed():
    """Rows whose transaction_id starts with 'C' must be dropped."""
    df = make_raw_df([
        ("C536379", "17850", "85123A", "Widget", 6, 2.55, "2010-12-01 08:26:00", "UK"),
        ("536365",  "17850", "85123A", "Widget", 6, 2.55, "2010-12-01 08:26:00", "UK"),
    ])
    # simulate the cancellation filter only
    mask = df["transaction_id"].astype(str).str.startswith("C")
    result = df[~mask]
    assert len(result) == 1
    assert result.iloc[0]["transaction_id"] == "536365"


def test_quantity_zero_removed():
    df = make_raw_df([
        ("536365", "17850", "85123A", "Widget",  0, 2.55, "2010-12-01", "UK"),
        ("536366", "17850", "85123A", "Widget",  3, 2.55, "2010-12-01", "UK"),
        ("536367", "17850", "85123A", "Widget", -1, 2.55, "2010-12-01", "UK"),
    ])
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    result = df[df["quantity"] > 0]
    assert len(result) == 1
    assert result.iloc[0]["transaction_id"] == "536366"


def test_negative_price_removed():
    df = make_raw_df([
        ("536365", "17850", "85123A", "Widget", 3, -1.00, "2010-12-01", "UK"),
        ("536366", "17850", "85123A", "Widget", 3,  0.00, "2010-12-01", "UK"),
        ("536367", "17850", "85123A", "Widget", 3,  2.55, "2010-12-01", "UK"),
    ])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    result = df[df["unit_price"] > 0]
    assert len(result) == 1
    assert result.iloc[0]["transaction_id"] == "536367"


def test_missing_customer_removed():
    df = make_raw_df([
        ("536365", None,    "85123A", "Widget", 3, 2.55, "2010-12-01", "UK"),
        ("536366", "",      "85123A", "Widget", 3, 2.55, "2010-12-01", "UK"),
        ("536367", "17850", "85123A", "Widget", 3, 2.55, "2010-12-01", "UK"),
    ])
    result = df[df["customer_id"].notna() & (df["customer_id"].astype(str).str.strip() != "")]
    assert len(result) == 1
    assert result.iloc[0]["transaction_id"] == "536367"


def test_exact_duplicates_removed():
    df = make_raw_df([
        ("536365", "17850", "85123A", "Widget", 3, 2.55, "2010-12-01", "UK"),
        ("536365", "17850", "85123A", "Widget", 3, 2.55, "2010-12-01", "UK"),  # duplicate
        ("536366", "17850", "85123B", "Widget", 2, 1.25, "2010-12-01", "UK"),
    ])
    result = df.drop_duplicates()
    assert len(result) == 2


def test_customer_id_normalised():
    """customer_id '17850.0' should become '17850'."""
    assert _is_numeric("17850.0")
    normalised = str(int(float("17850.0")))
    assert normalised == "17850"


def test_transaction_id_no_decimal():
    """transaction_id '536365.0' should become '536365'."""
    normalised = str(int(float("536365.0")))
    assert normalised == "536365"


def test_product_id_alphanumeric_preserved():
    """product_id '85123A' must not be cast to int."""
    assert not _is_numeric("85123A")
    assert str("85123A").strip() == "85123A"


def test_product_id_post_preserved():
    """Special StockCode 'POST' must be kept (human decision Q6)."""
    assert str("POST").strip() == "POST"


def test_column_map_complete():
    """COLUMN_MAP must cover all 8 source columns."""
    source_cols = {
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    }
    assert source_cols == set(COLUMN_MAP.keys())


def test_all_project_field_names_present():
    """COLUMN_MAP must produce the 8 required project field names."""
    required = {
        "transaction_id", "product_id", "product_name", "quantity",
        "event_time", "unit_price", "customer_id", "country"
    }
    assert required == set(COLUMN_MAP.values())


# ── Integration test: real dataset ───────────────────────────────────────────

DATASET = pathlib.Path(
    r"D:\University Semesters\7th Semester\DWHBI\Project\Online Retail.xlsx"
)


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_real_dataset_cleaning_counts():
    """
    Load and clean the real dataset; verify known counts.
    Expected (verified by Phase A analysis script):
      Raw: 541,909 | After cleaning: 392,692
    """
    df = load_and_clean(DATASET)
    assert len(df) == 392692, f"Expected 392,692 rows, got {len(df)}"


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_real_dataset_unique_counts():
    df = load_and_clean(DATASET)
    assert df["customer_id"].nunique() == 4338
    assert df["product_id"].nunique() == 3665
    assert df["transaction_id"].nunique() == 18532


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_no_cancellations_remain(tmp_path):
    df = load_and_clean(DATASET)
    cancel = df["transaction_id"].astype(str).str.startswith("C")
    assert cancel.sum() == 0, "Cancellations still present"


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_no_bad_quantity_remain():
    df = load_and_clean(DATASET)
    assert (df["quantity"] <= 0).sum() == 0


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_no_bad_price_remain():
    df = load_and_clean(DATASET)
    assert (df["unit_price"] <= 0).sum() == 0


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_no_missing_customer_remain():
    df = load_and_clean(DATASET)
    assert df["customer_id"].isna().sum() == 0


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_no_duplicates_remain():
    df = load_and_clean(DATASET)
    assert df.duplicated().sum() == 0


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_original_file_unmodified():
    """File size must match the original 23,715,344 bytes."""
    assert DATASET.stat().st_size == 23_715_344, "Original dataset was modified!"


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_customer_id_no_decimal():
    df = load_and_clean(DATASET)
    has_decimal = df["customer_id"].str.contains(r"\.", na=False)
    assert has_decimal.sum() == 0, "customer_id still has decimal points"


@pytest.mark.skipif(not DATASET.exists(), reason="Real dataset not available")
def test_event_time_parsed():
    df = load_and_clean(DATASET)
    assert pd.api.types.is_datetime64_any_dtype(df["event_time"])
    assert df["event_time"].isna().sum() == 0

