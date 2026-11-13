"""
Module 4 – Master Data Generation
Generates synthetic-but-deterministic customer and product master data
from the cleaned dataset, writes CSVs, and loads them into MySQL.
"""

import hashlib
import math
import os
import sys

# ── path so we can import cleaning & config ──────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
from cleaning import load_and_clean          # noqa: E402
from config import DATA_DIR, DATASET_PATH, get_connection      # noqa: E402

# ── DB defaults (host/port/db/user — password entered at runtime) ─────────────
_DB_HOST = "localhost"
_DB_PORT = 3306
_DB_NAME = "retail_dw"
_DB_USER = "root"

# ─────────────────────────────────────────────────────────────────────────────
# Deterministic hash helper
# ─────────────────────────────────────────────────────────────────────────────

def stable(text: str, n: int) -> int:
    """Return int(MD5(text)[:8], 16) % n  — deterministic, portable."""
    h = hashlib.md5(str(text).encode("utf-8")).hexdigest()[:8]
    return int(h, 16) % n


# ─────────────────────────────────────────────────────────────────────────────
# Lookup tables
# ─────────────────────────────────────────────────────────────────────────────

GENDERS        = ["Male", "Female"]
AGE_GROUPS     = ["0-17", "18-25", "26-35", "36-45", "46-50", "51-55", "55+"]
OCCUPATIONS    = ["Student", "Engineer", "Teacher", "Executive", "Clerk",
                  "Healthcare", "Retail", "Homemaker", "Artist", "Other"]
CITY_CATS      = ["A", "B", "C"]
STAY_YEARS     = ["0", "1", "2", "3", "4+"]
MARITAL        = ["Single", "Married"]

PRODUCT_CATS   = ["Home Decor", "Kitchen", "Stationery", "Toys",
                  "Seasonal Gifts", "Bags", "Jewelry", "Garden", "Party", "Other"]
SUPPLIERS      = [
    ("SUP01", "Northwind Supplies"),
    ("SUP02", "Atlantic Gifts Ltd"),
    ("SUP03", "Midlands Wholesale"),
    ("SUP04", "Celtic Craft Co"),
    ("SUP05", "EuroPack Partners"),
]

PARTITION_SIZE = 500


# ─────────────────────────────────────────────────────────────────────────────
# Customer master data
# ─────────────────────────────────────────────────────────────────────────────

def build_customer_md(df):
    """
    Returns list of dicts, one per unique customer_id (file order for country).
    customer_id stored as plain int string (no .0).
    """
    # First occurrence of country per customer (file order preserved after clean)
    first_country = (
        df.groupby("customer_id", sort=False)["country"]
        .first()
        .to_dict()
    )

    customers = []
    seen = set()
    for cid_raw in df["customer_id"]:
        cid = str(int(cid_raw))
        if cid in seen:
            continue
        seen.add(cid)
        country = str(first_country[cid_raw])
        customers.append({
            "customer_id":               cid,
            "gender":                    GENDERS[stable(cid, 2)],
            "age_group":                 AGE_GROUPS[stable(cid + "age", 7)],
            "occupation":                OCCUPATIONS[stable(cid + "occ", 10)],
            "city_category":             CITY_CATS[stable(cid + country, 3)],
            "stay_in_current_city_years": STAY_YEARS[stable(cid + "stay", 5)],
            "marital_status":            MARITAL[stable(cid + "mar", 2)],
        })
    return customers


# ─────────────────────────────────────────────────────────────────────────────
# Product master data
# ─────────────────────────────────────────────────────────────────────────────

def build_product_md(df):
    """
    Returns list of dicts, one per unique product_id.
    product_name = longest description per product_id; ties -> first in file order.
    """
    # Collect all descriptions per product in file order
    desc_col = "product_name" if "product_name" in df.columns else "description"
    product_descs: dict = {}
    for pid_raw, desc_raw in zip(df["product_id"], df[desc_col]):
        pid = str(pid_raw)
        desc = str(desc_raw).strip() if desc_raw and str(desc_raw).strip() else ""
        if pid not in product_descs:
            product_descs[pid] = []
        product_descs[pid].append(desc)

    # Unique product_ids in file order
    seen = set()
    ordered_pids = []
    for pid_raw in df["product_id"]:
        pid = str(pid_raw)
        if pid not in seen:
            seen.add(pid)
            ordered_pids.append(pid)

    products = []
    for pid in ordered_pids:
        descs = product_descs[pid]
        # Longest description; ties -> first occurrence (already in file order)
        best_len = max((len(d) for d in descs), default=0)
        name = next((d for d in descs if len(d) == best_len), "")
        if not name:
            name = "UNKNOWN"

        sup_idx = stable(pid, 5)
        sup_id, sup_name = SUPPLIERS[sup_idx]

        products.append({
            "product_id":       pid,
            "product_name":     name,
            "product_category": PRODUCT_CATS[stable(pid + "cat", 10)],
            "supplier_id":      sup_id,
            "supplier_name":    sup_name,
        })
    return products


# ─────────────────────────────────────────────────────────────────────────────
# CSV writers
# ─────────────────────────────────────────────────────────────────────────────

def write_csv(records, path: str) -> None:
    import csv
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not records:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


# ─────────────────────────────────────────────────────────────────────────────
# MySQL loaders
# ─────────────────────────────────────────────────────────────────────────────

def load_customer_md_to_db(records, conn) -> None:
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE customer_master_data")
    sql = """
        INSERT INTO customer_master_data
            (customer_id, gender, age_group, occupation,
             city_category, stay_in_current_city_years, marital_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    rows = [(r["customer_id"], r["gender"], r["age_group"], r["occupation"],
             r["city_category"], r["stay_in_current_city_years"],
             r["marital_status"]) for r in records]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()


def load_product_md_to_db(records, conn) -> None:
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE product_master_data")
    sql = """
        INSERT INTO product_master_data
            (product_id, product_name, product_category, supplier_id, supplier_name)
        VALUES (%s, %s, %s, %s, %s)
    """
    rows = [(r["product_id"], r["product_name"], r["product_category"],
             r["supplier_id"], r["supplier_name"]) for r in records]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def generate_master_data(password=None):
    print("Loading cleaned dataset ...")
    df = load_and_clean(DATASET_PATH)

    # -- Customer MD --
    print("Building customer master data ...")
    customers = build_customer_md(df)
    n_cust = len(customers)
    cust_partitions = math.ceil(n_cust / PARTITION_SIZE)
    cust_csv = os.path.join(DATA_DIR, "customer_master_data.csv")
    write_csv(customers, cust_csv)
    print(f"  Customers : {n_cust:,}  ->  {cust_partitions} partitions  ->  {cust_csv}")

    # -- Product MD --
    print("Building product master data ...")
    products = build_product_md(df)
    n_prod = len(products)
    prod_partitions = math.ceil(n_prod / PARTITION_SIZE)
    prod_csv = os.path.join(DATA_DIR, "product_master_data.csv")
    write_csv(products, prod_csv)
    print(f"  Products  : {n_prod:,}  ->  {prod_partitions} partitions  ->  {prod_csv}")

    # -- MySQL load --
    if password is None:
        import getpass
        password = getpass.getpass("MySQL root password: ")

    conn = get_connection(_DB_HOST, _DB_PORT, _DB_NAME, _DB_USER, password)
    print("Loading customer_master_data into MySQL ...")
    load_customer_md_to_db(customers, conn)
    print("Loading product_master_data into MySQL ...")
    load_product_md_to_db(products, conn)
    conn.close()

    print("\nModule 4 complete.")
    print(f"  customer_master_data : {n_cust:,} rows  ({cust_partitions} partitions)")
    print(f"  product_master_data  : {n_prod:,} rows  ({prod_partitions} partitions)")
    return customers, products


if __name__ == "__main__":
    import getpass
    pw = getpass.getpass("MySQL root password: ")
    generate_master_data(password=pw)
