"""
Module 5 — Data Preparation
Assigns sales_mode (online/periodic) and store_id to every cleaned transaction,
writes the online stream CSV, and stages the periodic rows into MySQL.

Sales-mode rule (invoice level):
  hash = int(MD5(str(int(invoice_no)))[:8], 16) % 10
  0-6 -> online   (processing_mode = 'online',   sales_mode = 'online')
  7-9 -> periodic (processing_mode = 'periodic', sales_mode = 'periodic')

Store rule:
  online   -> store_id = 1
  periodic -> store_id = 2 + int(MD5(str(int(txn_id)))[:8], 16) % 4  (i.e. 2-5)

Periodic staging:
  Sort by event_time, split into 48 equal groups (last group may be larger),
  assign release_hour 0..47, insert into periodic_sales_data with released=False.
"""

import hashlib
import math
import os
import sys
import csv

import mysql.connector

sys.path.insert(0, os.path.dirname(__file__))
from cleaning import load_and_clean
from config import (
    DATA_DIR, DATASET_PATH, ONLINE_CSV, NUM_PERIODIC_GROUPS,
    STORE_LIST, get_connection,
)

# ── DB credentials (not hardcoded) ───────────────────────────────────────────
_DB_HOST = "localhost"
_DB_PORT = 3306
_DB_NAME = "retail_dw"
_DB_USER = "root"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _md5_int(text: str) -> int:
    return int(hashlib.md5(str(text).encode("utf-8")).hexdigest()[:8], 16)


def assign_sales_mode(invoice_no) -> str:
    """Return 'online' or 'periodic' based on invoice_no hash."""
    return "online" if _md5_int(str(int(invoice_no))) % 10 <= 6 else "periodic"


def assign_store_id(sales_mode: str, txn_id) -> int:
    """Return store_id: 1 for online; 2-5 for periodic."""
    if sales_mode == "online":
        return 1
    return 2 + _md5_int(str(int(txn_id))) % 4


# ─────────────────────────────────────────────────────────────────────────────
# Core preparation
# ─────────────────────────────────────────────────────────────────────────────

def prepare_data(df):
    """
    Takes a cleaned DataFrame and returns (online_df, periodic_df)
    with 'sales_mode', 'processing_mode', and 'store_id' columns added.
    """
    # Build invoice-level mode map (all rows in same invoice share same mode)
    invoice_mode = {}
    for inv in df["transaction_id"].unique():
        invoice_mode[str(inv)] = assign_sales_mode(inv)

    df = df.copy()
    df["sales_mode"]      = df["transaction_id"].apply(lambda x: invoice_mode[str(x)])
    df["processing_mode"] = df["sales_mode"]   # same value per decision #1
    df["store_id"]        = df.apply(
        lambda r: assign_store_id(r["sales_mode"], r["transaction_id"]), axis=1
    )

    online_df   = df[df["sales_mode"] == "online"].reset_index(drop=True)
    periodic_df = df[df["sales_mode"] == "periodic"].reset_index(drop=True)
    return online_df, periodic_df


# ─────────────────────────────────────────────────────────────────────────────
# Online CSV writer
# ─────────────────────────────────────────────────────────────────────────────

ONLINE_COLUMNS = [
    "transaction_id", "customer_id", "product_id",
    "quantity", "unit_price", "event_time", "sales_mode",
]


def write_online_csv(online_df, path: str = ONLINE_CSV) -> int:
    """Write online rows to CSV. Returns row count."""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(ONLINE_COLUMNS)
        for _, row in online_df.iterrows():
            writer.writerow([
                row["transaction_id"],
                str(int(row["customer_id"])),
                row["product_id"],
                row["quantity"],
                row["unit_price"],
                row["event_time"].strftime("%Y-%m-%d %H:%M:%S"),
                row["sales_mode"],
            ])
    return len(online_df)


# ─────────────────────────────────────────────────────────────────────────────
# Periodic staging
# ─────────────────────────────────────────────────────────────────────────────

def stage_periodic(periodic_df, conn) -> int:
    """
    Sort by event_time, split into NUM_PERIODIC_GROUPS (48) groups,
    assign release_hour 0..47, insert into periodic_sales_data.
    Returns row count inserted.
    """
    df = periodic_df.sort_values("event_time").reset_index(drop=True)
    n  = len(df)
    group_size = math.floor(n / NUM_PERIODIC_GROUPS)  # last group absorbs remainder

    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE periodic_sales_data")

    sql = """
        INSERT INTO periodic_sales_data
            (transaction_id, customer_id, product_id, quantity, unit_price,
             event_time, sales_mode, processing_mode, store_id, release_hour, released)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    batch = []
    for i, (_, row) in enumerate(df.iterrows()):
        release_hour = min(i // group_size, NUM_PERIODIC_GROUPS - 1)
        batch.append((
            str(row["transaction_id"]),
            str(int(row["customer_id"])),
            str(row["product_id"]),
            int(row["quantity"]),
            float(row["unit_price"]),
            row["event_time"].strftime("%Y-%m-%d %H:%M:%S"),
            row["sales_mode"],
            row["processing_mode"],
            int(row["store_id"]),
            release_hour,
            False,
        ))
        if len(batch) >= 5000:
            cur.executemany(sql, batch)
            conn.commit()
            batch = []

    if batch:
        cur.executemany(sql, batch)
        conn.commit()

    cur.close()
    return n


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_data_prep(password=None):
    print("Loading cleaned dataset ...")
    df = load_and_clean(DATASET_PATH)

    print("Assigning sales modes and store IDs ...")
    online_df, periodic_df = prepare_data(df)

    n_online   = len(online_df)
    n_periodic = len(periodic_df)
    print(f"  Online   rows : {n_online:,}")
    print(f"  Periodic rows : {n_periodic:,}")

    # Online CSV
    print(f"Writing online CSV -> {ONLINE_CSV} ...")
    write_online_csv(online_df)
    print(f"  Written {n_online:,} rows.")

    # Periodic staging
    if password is None:
        import getpass
        password = getpass.getpass("MySQL root password: ")

    conn = get_connection(_DB_HOST, _DB_PORT, _DB_NAME, _DB_USER, password)
    print("Staging periodic data into MySQL ...")
    inserted = stage_periodic(periodic_df, conn)
    conn.close()
    print(f"  Staged {inserted:,} periodic rows into periodic_sales_data.")

    group_size_approx = math.floor(n_periodic / NUM_PERIODIC_GROUPS)
    print(f"\nModule 5 complete.")
    print(f"  Online   : {n_online:,}  |  Periodic : {n_periodic:,}")
    print(f"  Periodic group size (approx) : {group_size_approx:,} rows/hour")
    return online_df, periodic_df


if __name__ == "__main__":
    import getpass
    pw = getpass.getpass("MySQL root password: ")
    run_data_prep(password=pw)
