"""
tests/test_m2_schema.py — Module 2 acceptance tests.
Verifies: all tables/view exist, FK constraints, correct columns, idempotency.
"""
import pytest
import mysql.connector
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="module")
def conn():
    c = mysql.connector.connect(
        host="localhost", port=3306, database="retail_dw",
        user="root", password="8956"
    )
    yield c
    c.close()


@pytest.fixture(scope="module")
def cursor(conn):
    cur = conn.cursor()
    yield cur
    cur.close()


def get_tables(cursor):
    cursor.execute("SHOW TABLES")
    return {row[0].upper() for row in cursor.fetchall()}


def get_columns(cursor, table):
    cursor.execute(f"SHOW COLUMNS FROM {table}")
    return {row[0] for row in cursor.fetchall()}


# ── Table existence ───────────────────────────────────────────────────────────

def test_dim_customer_exists(cursor):
    assert "DIM_CUSTOMER" in get_tables(cursor)

def test_dim_product_exists(cursor):
    assert "DIM_PRODUCT" in get_tables(cursor)

def test_dim_supplier_exists(cursor):
    assert "DIM_SUPPLIER" in get_tables(cursor)

def test_dim_store_exists(cursor):
    assert "DIM_STORE" in get_tables(cursor)

def test_dim_time_exists(cursor):
    assert "DIM_TIME" in get_tables(cursor)

def test_fact_sales_exists(cursor):
    assert "FACT_SALES" in get_tables(cursor)

def test_periodic_staging_exists(cursor):
    assert "PERIODIC_SALES_DATA" in get_tables(cursor)

def test_shed_table_exists(cursor):
    assert "SHED_TABLE" in get_tables(cursor)

def test_customer_md_table_exists(cursor):
    assert "CUSTOMER_MASTER_DATA" in get_tables(cursor)

def test_product_md_table_exists(cursor):
    assert "PRODUCT_MASTER_DATA" in get_tables(cursor)


# ── View existence ────────────────────────────────────────────────────────────

def test_store_quarterly_view_exists(cursor):
    cursor.execute(
        "SELECT TABLE_NAME FROM information_schema.VIEWS "
        "WHERE TABLE_SCHEMA = 'retail_dw' AND TABLE_NAME = 'STORE_QUARTERLY_SALES'"
    )
    assert cursor.fetchone() is not None


# ── FACT_SALES columns ────────────────────────────────────────────────────────

def test_fact_sales_columns(cursor):
    cols = get_columns(cursor, "FACT_SALES")
    required = {
        "fact_key", "transaction_id", "customer_key", "product_key",
        "supplier_key", "store_key", "time_key",
        "quantity", "revenue", "sales_mode", "processing_mode",
    }
    assert required.issubset(cols), f"Missing columns: {required - cols}"


# ── DIM_TIME columns ──────────────────────────────────────────────────────────

def test_dim_time_columns(cursor):
    cols = get_columns(cursor, "DIM_TIME")
    required = {
        "time_key", "event_date", "event_hour", "day_of_month",
        "month", "quarter", "year", "day_of_week",
        "is_weekend", "season", "half_year",
    }
    assert required.issubset(cols), f"Missing: {required - cols}"


# ── DIM_TIME unique constraint on (event_date, event_hour) ───────────────────

def test_dim_time_unique_date_hour(cursor, conn):
    cursor.execute("DELETE FROM DIM_TIME WHERE event_date = '2010-12-01' AND event_hour = 8")
    conn.commit()
    cursor.execute(
        "INSERT INTO DIM_TIME (event_date, event_hour, day_of_month, month, quarter, year, "
        "day_of_week, is_weekend, season, half_year) "
        "VALUES ('2010-12-01', 8, 1, 12, 4, 2010, 'Wednesday', 0, 'Winter', 'H2')"
    )
    conn.commit()
    with pytest.raises(mysql.connector.errors.IntegrityError):
        cursor.execute(
            "INSERT INTO DIM_TIME (event_date, event_hour, day_of_month, month, quarter, year, "
            "day_of_week, is_weekend, season, half_year) "
            "VALUES ('2010-12-01', 8, 1, 12, 4, 2010, 'Wednesday', 0, 'Winter', 'H2')"
        )
    conn.rollback()
    cursor.execute("DELETE FROM DIM_TIME WHERE event_date = '2010-12-01' AND event_hour = 8")
    conn.commit()


# ── FK constraint: fact row with invalid customer_key must fail ───────────────

def test_fact_fk_enforced(cursor, conn):
    with pytest.raises(mysql.connector.errors.IntegrityError):
        cursor.execute(
            "INSERT INTO FACT_SALES "
            "(transaction_id, customer_key, product_key, supplier_key, store_key, time_key, "
            "quantity, revenue, sales_mode, processing_mode) "
            "VALUES ('TEST01', 99999, 99999, 99999, 99999, 99999, 1, 10.00, 'online', 'online')"
        )
    conn.rollback()


# ── Idempotency: re-run schema, all tables still exist ───────────────────────

def test_schema_idempotent(cursor, conn):
    from db_setup import run_schema
    run_schema("localhost", 3306, "retail_dw", "root", "8956")
    assert "DIM_CUSTOMER" in get_tables(cursor)
    assert "FACT_SALES"   in get_tables(cursor)


# ── periodic_sales_data has release_hour and released columns ────────────────

def test_periodic_staging_columns(cursor):
    cols = get_columns(cursor, "periodic_sales_data")
    assert "release_hour" in cols
    assert "released" in cols
    assert "event_time"  in cols
    assert "sales_mode"  in cols


# ── shed_table has flag and shed_time ────────────────────────────────────────

def test_shed_table_columns(cursor):
    cols = get_columns(cursor, "shed_table")
    assert "flag"      in cols
    assert "shed_time" in cols
    assert "sales_mode" in cols
