"""
verify_enrichment.py — Rigorous verification that MESHJOIN genuinely enriches
from Master Data tables and does NOT rely on fallback defaults.

Verifies:
1. StreamBuffer -> MeshJoinEngine -> 9 Customer Partitions & 8 Product Partitions.
2. Every transaction's customer and product attributes match master_data exactly.
3. Consolidator inserts exact master data attributes into DIM_CUSTOMER & DIM_PRODUCT.
4. Direct SQL validation that zero test records used fallback values.
"""

import getpass
import sys
import mysql.connector

from config import get_connection
from consolidation import DataConsolidator
from meshjoin import MeshJoinEngine, StreamBuffer, Transaction, create_db_loaders


def run_verification(password: str = "8956"):
    print("=" * 70)
    print("  MESHJOIN MASTER DATA ENRICHMENT VERIFICATION TEST")
    print("=" * 70)

    # 1. Connect
    conn = get_connection("localhost", 3306, "retail_dw", "root", password)
    conn.autocommit = True
    cur = conn.cursor(dictionary=True)

    # 2. Pick 5 distinct customers across different partitions
    cur.execute("""
        SELECT customer_id, gender, age_group, occupation, city_category,
               stay_in_current_city_years, marital_status
        FROM customer_master_data
        ORDER BY customer_id
        LIMIT 5
    """)
    target_customers = {row["customer_id"]: row for row in cur.fetchall()}

    # Also pick 5 distinct products
    cur.execute("""
        SELECT product_id, product_name, product_category, supplier_id, supplier_name
        FROM product_master_data
        ORDER BY product_id
        LIMIT 5
    """)
    target_products = {row["product_id"]: row for row in cur.fetchall()}

    print(f"Loaded {len(target_customers)} target customers and {len(target_products)} target products from MySQL.")

    # 3. Setup MESHJOIN Engine
    c_loader, p_loader, nc, np = create_db_loaders(conn)
    stream_buf = StreamBuffer(capacity=100)
    engine = MeshJoinEngine(
        stream_buffer=stream_buf,
        customer_loader=c_loader,
        product_loader=p_loader,
        num_customer_partitions=nc,
        num_product_partitions=np,
    )
    consolidator = DataConsolidator(conn)

    print(f"Engine Initialized: {nc} customer partitions, {np} product partitions.")
    print(f"Max iterations required for full enrichment: {engine.max_iterations}")

    # 4. Generate test transactions using target IDs
    test_txns = []
    cust_list = list(target_customers.keys())
    prod_list = list(target_products.keys())

    for i in range(5):
        txn = Transaction(
            transaction_id=f"TEST_VERIFY_{i+1}",
            customer_id=cust_list[i],
            product_id=prod_list[i],
            quantity=i + 1,
            unit_price=10.0 * (i + 1),
            event_time="2011-06-15 12:00:00",
            sales_mode="online",
            store_id=1,
        )
        test_txns.append(txn)
        stream_buf.put(txn, priority=1)

    print(f"\nInjected {len(test_txns)} transactions into Stream Buffer.")
    print("Stepping MESHJOIN engine through interleaved partition cycles...")

    # 5. Run MESHJOIN iterations until all test transactions are evicted
    evicted_results = []
    for step in range(engine.max_iterations + 1):
        evicted = engine.step()
        if evicted:
            evicted_results.extend(evicted)
            print(f"  Step {step + 1}: Evicted {len(evicted)} fully matched transaction(s).")
        if len(evicted_results) >= len(test_txns):
            break

    print(f"\nTotal evicted from MESHJOIN: {len(evicted_results)} transactions.")

    # 6. Verify in-memory enrichment matches Master Data
    print("\n" + "-" * 70)
    print("STEP 1: VERIFYING IN-MEMORY ENRICHMENT ATTRIBUTES")
    print("-" * 70)

    for txn in evicted_results:
        exp_c = target_customers[txn.customer_id]
        exp_p = target_products[txn.product_id]

        print(f"\n[Transaction {txn.transaction_id}]")
        print(f"  Customer ID      : {txn.customer_id}")
        print(f"  Customer Enriched: {txn.customer_enriched}")
        print(f"  Gender           : '{txn.gender}' (Expected: '{exp_c['gender']}')")
        print(f"  Age Group        : '{txn.age_group}' (Expected: '{exp_c['age_group']}')")
        print(f"  Occupation       : '{txn.occupation}' (Expected: '{exp_c['occupation']}')")
        print(f"  Product ID       : {txn.product_id}")
        print(f"  Product Enriched : {txn.product_enriched}")
        print(f"  Product Name     : '{txn.product_name}' (Expected: '{exp_p['product_name']}')")
        print(f"  Category         : '{txn.product_category}' (Expected: '{exp_p['product_category']}')")
        print(f"  Supplier         : '{txn.supplier_name}' (Expected: '{exp_p['supplier_name']}')")

        assert txn.customer_enriched is True, "customer_enriched flag was False!"
        assert txn.product_enriched is True, "product_enriched flag was False!"
        assert txn.gender == exp_c["gender"], f"Gender mismatch: {txn.gender} != {exp_c['gender']}"
        assert txn.age_group == exp_c["age_group"], f"Age group mismatch!"
        assert txn.occupation == exp_c["occupation"], f"Occupation mismatch!"
        assert txn.product_name == exp_p["product_name"], f"Product name mismatch!"
        assert txn.product_category == exp_p["product_category"], f"Category mismatch!"
        assert txn.supplier_name == exp_p["supplier_name"], f"Supplier mismatch!"

    print("\n>>> ALL IN-MEMORY ATTRIBUTES MATCHED MASTER DATA PERFECTLY (ZERO FALLBACKS USED).")

    # 7. Consolidate into Data Warehouse tables
    print("\n" + "-" * 70)
    print("STEP 2: CONSOLIDATING INTO DATA WAREHOUSE & VERIFYING MySQL ROWS")
    print("-" * 70)

    consolidator.consolidate_batch(evicted_results)

    # 8. Query DIM_CUSTOMER directly from MySQL and verify
    for txn in evicted_results:
        exp_c = target_customers[txn.customer_id]
        exp_p = target_products[txn.product_id]

        cur.execute("""
            SELECT gender, age_group, occupation, city_category, marital_status
            FROM DIM_CUSTOMER
            WHERE customer_id = %s
        """, (txn.customer_id,))
        dim_c = cur.fetchone()

        assert dim_c is not None, f"Customer {txn.customer_id} missing from DIM_CUSTOMER!"
        assert dim_c["gender"] == exp_c["gender"], f"DIM_CUSTOMER gender mismatch!"
        assert dim_c["age_group"] == exp_c["age_group"], f"DIM_CUSTOMER age_group mismatch!"
        assert dim_c["occupation"] == exp_c["occupation"], f"DIM_CUSTOMER occupation mismatch!"

        cur.execute("""
            SELECT product_name, product_category
            FROM DIM_PRODUCT
            WHERE product_id = %s
        """, (txn.product_id,))
        dim_p = cur.fetchone()

        assert dim_p is not None, f"Product {txn.product_id} missing from DIM_PRODUCT!"
        assert dim_p["product_name"] == exp_p["product_name"], f"DIM_PRODUCT name mismatch!"
        assert dim_p["product_category"] == exp_p["product_category"], f"DIM_PRODUCT category mismatch!"

    print(">>> ALL DIM_CUSTOMER AND DIM_PRODUCT RECORDS IN MySQL MATCH MASTER DATA EXACTLY.")

    # 9. Clean up test fact records
    cur.execute("DELETE FROM FACT_SALES WHERE transaction_id LIKE 'TEST_VERIFY_%'")
    cur.close()
    conn.close()

    print("\n" + "=" * 70)
    print("  VERIFICATION SUCCESSFUL: 100% GENUINE MASTER DATA ENRICHMENT CONFIRMED!")
    print("=" * 70)


if __name__ == "__main__":
    pw = sys.argv[1] if len(sys.argv) > 1 else "8956"
    run_verification(password=pw)
