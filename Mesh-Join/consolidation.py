"""
consolidation.py — Revenue calculation, dimension upsert, and FACT_SALES loading.

Responsibilities:
1. Derives full temporal hierarchy for DIM_TIME:
   date, hour, day_of_month, month, quarter, year, day_of_week, is_weekend, season, half_year.
2. Manages fast in-memory dimension key caches to avoid redundant DB queries.
3. Performs dimension upserts (DIM_CUSTOMER, DIM_PRODUCT, DIM_SUPPLIER, DIM_STORE, DIM_TIME).
4. Populates FACT_SALES with resolved dimension foreign keys, revenue = quantity * unit_price,
   and ensures processing_mode = sales_mode.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config import STORE_LIST
from meshjoin import Transaction


# ─────────────────────────────────────────────────────────────────────────────
# 1. Temporal Attribute Derivation
# ─────────────────────────────────────────────────────────────────────────────

def get_season(month: int) -> str:
    """Mar-May = Spring, Jun-Aug = Summer, Sep-Nov = Fall, Dec-Feb = Winter."""
    if 3 <= month <= 5:
        return "Spring"
    elif 6 <= month <= 8:
        return "Summer"
    elif 9 <= month <= 11:
        return "Fall"
    else:
        return "Winter"


def derive_time_attributes(event_time: Any) -> Dict[str, Any]:
    """
    Parse event_time and extract all DIM_TIME attributes.
    Supports datetime objects, pandas Timestamps, and strings.
    """
    if isinstance(event_time, str):
        try:
            dt = datetime.datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dt = pd.to_datetime(event_time).to_pydatetime()
    elif hasattr(event_time, "to_pydatetime"):
        dt = event_time.to_pydatetime()
    elif isinstance(event_time, datetime.datetime):
        dt = event_time
    else:
        dt = pd.to_datetime(event_time).to_pydatetime()

    event_date = dt.date()
    event_hour = dt.hour
    day_of_month = dt.day
    month = dt.month
    quarter = (month - 1) // 3 + 1
    year = dt.year
    day_of_week = dt.strftime("%A")
    is_weekend = dt.weekday() in (5, 6)
    season = get_season(month)
    half_year = "H1" if month <= 6 else "H2"

    return {
        "event_date": event_date,
        "event_hour": event_hour,
        "day_of_month": day_of_month,
        "month": month,
        "quarter": quarter,
        "year": year,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "season": season,
        "half_year": half_year,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Data Consolidator
# ─────────────────────────────────────────────────────────────────────────────

class DataConsolidator:
    """
    Coordinates dimension upserts and bulk loading into FACT_SALES.
    Uses in-memory dictionaries for dimension key lookups to maximize performance.
    """

    def __init__(self, conn=None):
        self.conn = conn
        # Caches: natural_key -> surrogate_key
        self.customer_cache: Dict[str, int] = {}
        self.product_cache: Dict[str, int] = {}
        self.supplier_cache: Dict[str, int] = {}
        self.store_cache: Dict[int, int] = {}
        self.time_cache: Dict[Tuple[str, int], int] = {}

        if self.conn is not None:
            self._warm_caches()

    def _warm_caches(self) -> None:
        """Pre-load existing dimension surrogate keys into memory."""
        cur = self.conn.cursor()

        # Customer
        cur.execute("SELECT customer_id, customer_key FROM DIM_CUSTOMER")
        for cid, key in cur.fetchall():
            self.customer_cache[str(cid)] = key

        # Product
        cur.execute("SELECT product_id, product_key FROM DIM_PRODUCT")
        for pid, key in cur.fetchall():
            self.product_cache[str(pid)] = key

        # Supplier
        cur.execute("SELECT supplier_id, supplier_key FROM DIM_SUPPLIER")
        for sid, key in cur.fetchall():
            self.supplier_cache[str(sid)] = key

        # Store
        cur.execute("SELECT store_id, store_key FROM DIM_STORE")
        for sid, key in cur.fetchall():
            self.store_cache[int(sid)] = key

        # Time
        cur.execute("SELECT event_date, event_hour, time_key FROM DIM_TIME")
        for dt, hr, key in cur.fetchall():
            self.time_cache[(str(dt), int(hr))] = key

        cur.close()

    def get_or_create_customer(self, txn: Transaction, cur) -> int:
        cid = str(txn.customer_id)
        if cid in self.customer_cache:
            return self.customer_cache[cid]

        sql = """
            INSERT INTO DIM_CUSTOMER
                (customer_id, gender, age_group, occupation, city_category,
                 stay_in_current_city_years, marital_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE customer_key = LAST_INSERT_ID(customer_key)
        """
        cur.execute(
            sql,
            (
                cid,
                txn.gender or "Other",
                txn.age_group or "Unknown",
                txn.occupation or "Other",
                txn.city_category or "B",
                txn.stay_in_current_city_years or "1",
                txn.marital_status or "Single",
            ),
        )
        key = cur.lastrowid
        self.customer_cache[cid] = key
        return key

    def get_or_create_product(self, txn: Transaction, supplier_key: int = 1, cur=None) -> int:
        pid = str(txn.product_id)
        if pid in self.product_cache:
            return self.product_cache[pid]

        sql = """
            INSERT INTO DIM_PRODUCT (product_id, product_name, product_category, supplier_key)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE product_key = LAST_INSERT_ID(product_key)
        """
        cur.execute(sql, (pid, txn.product_name or "UNKNOWN", txn.product_category or "Other", supplier_key))
        key = cur.lastrowid
        self.product_cache[pid] = key
        return key

    def get_or_create_supplier(self, txn: Transaction, cur) -> int:
        sid = str(txn.supplier_id or "SUP01")
        if sid in self.supplier_cache:
            return self.supplier_cache[sid]

        sql = """
            INSERT INTO DIM_SUPPLIER (supplier_id, supplier_name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE supplier_key = LAST_INSERT_ID(supplier_key)
        """
        cur.execute(sql, (sid, txn.supplier_name or "Unknown Supplier"))
        key = cur.lastrowid
        self.supplier_cache[sid] = key
        return key

    def get_or_create_store(self, txn: Transaction, cur) -> int:
        sid = int(txn.store_id)
        if sid in self.store_cache:
            return self.store_cache[sid]

        store_info = STORE_LIST.get(sid, {"store_name": f"Store {sid}", "city": "Unknown"})
        sql = """
            INSERT INTO DIM_STORE (store_id, store_name, city)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE store_key = LAST_INSERT_ID(store_key)
        """
        cur.execute(sql, (sid, store_info["store_name"], store_info["city"]))
        key = cur.lastrowid
        self.store_cache[sid] = key
        return key

    def get_or_create_time(self, event_time: Any, cur) -> int:
        t_attrs = derive_time_attributes(event_time)
        cache_key = (str(t_attrs["event_date"]), int(t_attrs["event_hour"]))
        if cache_key in self.time_cache:
            return self.time_cache[cache_key]

        sql = """
            INSERT INTO DIM_TIME
                (event_date, event_hour, day_of_month, month, quarter, year,
                 day_of_week, is_weekend, season, half_year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE time_key = LAST_INSERT_ID(time_key)
        """
        cur.execute(
            sql,
            (
                t_attrs["event_date"],
                t_attrs["event_hour"],
                t_attrs["day_of_month"],
                t_attrs["month"],
                t_attrs["quarter"],
                t_attrs["year"],
                t_attrs["day_of_week"],
                t_attrs["is_weekend"],
                t_attrs["season"],
                t_attrs["half_year"],
            ),
        )
        key = cur.lastrowid
        self.time_cache[cache_key] = key
        return key

    def consolidate_batch(self, transactions: List[Transaction]) -> int:
        """
        Process a batch of enriched transactions:
        1. Resolve dimension keys (creating dimension rows if missing)
        2. Batch insert into FACT_SALES
        Returns count of fact rows inserted.
        """
        if not transactions or self.conn is None:
            return len(transactions)

        cur = self.conn.cursor()
        fact_rows = []

        for txn in transactions:
            c_key = self.get_or_create_customer(txn, cur)
            s_key = self.get_or_create_supplier(txn, cur)
            p_key = self.get_or_create_product(txn, s_key, cur)
            st_key = self.get_or_create_store(txn, cur)
            t_key = self.get_or_create_time(txn.event_time, cur)

            revenue = round(float(txn.quantity) * float(txn.unit_price), 2)
            sales_mode = str(txn.sales_mode)
            processing_mode = sales_mode  # Always identical per human decision #1

            fact_rows.append((
                str(txn.transaction_id),
                c_key,
                p_key,
                s_key,
                st_key,
                t_key,
                int(txn.quantity),
                revenue,
                sales_mode,
                processing_mode,
            ))

        sql_fact = """
            INSERT INTO FACT_SALES
                (transaction_id, customer_key, product_key, supplier_key, store_key,
                 time_key, quantity, revenue, sales_mode, processing_mode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(sql_fact, fact_rows)
        self.conn.commit()
        cur.close()

        return len(fact_rows)
