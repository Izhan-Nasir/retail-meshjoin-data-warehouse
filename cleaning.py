"""
cleaning.py — Read the raw Online Retail dataset, apply all cleaning rules,
remove exact duplicates, and return a clean pandas DataFrame.

Cleaning order (manual §3 + human decision Q5):
  1. Remove cancellations  — transaction_id starts with 'C'
  2. Remove quantity <= 0
  3. Remove unit_price <= 0
  4. Remove missing customer_id
  5. Remove exact duplicate rows

Counts at each step are printed to stdout (required in report).
The original file is NEVER modified.
"""

import hashlib
import pathlib
import pandas as pd


# ── Field name mapping (source → project) ────────────────────────────────────
COLUMN_MAP = {
    "InvoiceNo":   "transaction_id",
    "StockCode":   "product_id",
    "Description": "product_name",
    "Quantity":    "quantity",
    "InvoiceDate": "event_time",
    "UnitPrice":   "unit_price",
    "CustomerID":  "customer_id",
    "Country":     "country",
}


def load_and_clean(filepath: str | pathlib.Path) -> pd.DataFrame:
    """
    Read the raw Excel file, apply all cleaning rules, return clean DataFrame.

    Printed output (also required in the project report):
      • Total rows before cleaning
      • Rows removed at each step
      • Total rows after cleaning
    """
    filepath = pathlib.Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Dataset not found: {filepath}")

    print(f"[cleaning] Reading: {filepath}")
    df = pd.read_excel(filepath, engine="openpyxl", dtype=str)
    # Read everything as str first to avoid pandas type-coercion surprises,
    # then cast numeric columns explicitly.

    # ── Rename columns ────────────────────────────────────────────────────────
    df = df.rename(columns=COLUMN_MAP)

    # ── Cast numeric columns ──────────────────────────────────────────────────
    df["quantity"]   = pd.to_numeric(df["quantity"],   errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    # event_time: parse datetime
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    total_raw = len(df)
    print(f"\n[cleaning] ── Cleaning report ──────────────────────")
    print(f"  Rows before cleaning : {total_raw:>8,}")

    # ── Step 1: Remove cancellations (transaction_id starts with 'C') ────────
    cancel_mask = df["transaction_id"].astype(str).str.startswith("C")
    n_cancel = cancel_mask.sum()
    df = df[~cancel_mask].copy()
    print(f"  Cancellations removed: {n_cancel:>8,}  (running total: {len(df):,})")

    # ── Step 2: Remove quantity <= 0 ─────────────────────────────────────────
    qty_bad = (df["quantity"] <= 0) | df["quantity"].isna()
    n_qty = qty_bad.sum()
    df = df[~qty_bad].copy()
    print(f"  Quantity ≤ 0 removed : {n_qty:>8,}  (running total: {len(df):,})")

    # ── Step 3: Remove unit_price <= 0 ───────────────────────────────────────
    price_bad = (df["unit_price"] <= 0) | df["unit_price"].isna()
    n_price = price_bad.sum()
    df = df[~price_bad].copy()
    print(f"  Unit price ≤ 0 removed:{n_price:>7,}  (running total: {len(df):,})")

    # ── Step 4: Remove missing customer_id ───────────────────────────────────
    cust_bad = df["customer_id"].isna() | (df["customer_id"].astype(str).str.strip() == "")
    n_cust = cust_bad.sum()
    df = df[~cust_bad].copy()
    print(f"  Missing customer_id  : {n_cust:>8,}  (running total: {len(df):,})")

    # ── Step 5: Remove exact duplicate rows (human decision Q5) ──────────────
    before_dedup = len(df)
    df = df.drop_duplicates()
    n_dedup = before_dedup - len(df)
    print(f"  Exact duplicates     : {n_dedup:>8,}  (running total: {len(df):,})")

    total_cleaned = len(df)
    total_removed = total_raw - total_cleaned
    print(f"  ────────────────────────────────────────────────")
    print(f"  Rows after cleaning  : {total_cleaned:>8,}")
    print(f"  Total rows removed   : {total_removed:>8,}")
    print()

    # ── Normalise ID columns ──────────────────────────────────────────────────
    # transaction_id: all remaining InvoiceNos are integers after removing 'C' rows
    df["transaction_id"] = df["transaction_id"].apply(
        lambda x: str(int(float(x))) if _is_numeric(x) else str(x)
    )
    # customer_id: float → int → str  (e.g. '17850.0' → '17850')
    df["customer_id"] = df["customer_id"].apply(
        lambda x: str(int(float(x))) if _is_numeric(x) else str(x)
    )
    # product_id: preserve alphanumeric codes (e.g. '85123A', 'POST')
    df["product_id"] = df["product_id"].astype(str).str.strip()

    # Reset index
    df = df.reset_index(drop=True)
    return df


def _is_numeric(val) -> bool:
    """Return True if val can be cast to float."""
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False


# ── Convenience: run as script ───────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from config import DATASET_PATH
    path = sys.argv[1] if len(sys.argv) > 1 else DATASET_PATH
    df = load_and_clean(path)
    print(f"[cleaning] Unique customers : {df['customer_id'].nunique():,}")
    print(f"[cleaning] Unique products  : {df['product_id'].nunique():,}")
    print(f"[cleaning] Unique invoices  : {df['transaction_id'].nunique():,}")
    print(f"[cleaning] Date range       : {df['event_time'].min()} → {df['event_time'].max()}")
