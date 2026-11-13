"""
run_queries.py — Interactive runner for all 20 OLAP queries in queries.sql.

Executes each query against the populated retail_dw database, displays
formatted result tables in the terminal, and exports results to query_results.txt.
"""

import getpass
import os
import re
import sys
import pandas as pd
import mysql.connector

from config import DB_CONFIG, get_connection

QUERIES_FILE = "queries.sql"
OUTPUT_FILE = "query_results.txt"


def parse_queries(filepath=QUERIES_FILE):
    """Parse queries.sql into individual (query_num, header, sql) tuples."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Split by query headers: -- Q<number>:
    pattern = r"(--\s*Q(\d+):[^\n]*\n(?:--[^\n]*\n)*)(.*?)(?=(?:--\s*Q\d+:)|$)"
    matches = re.findall(pattern, content, re.DOTALL)

    queries = []
    for header, num, sql in matches:
        clean_sql = sql.strip().rstrip(";")
        if clean_sql:
            queries.append((int(num), header.strip(), clean_sql))
    return queries


def run_all_queries(password):
    conn = mysql.connector.connect(
        host="localhost",
        port=3306,
        database="retail_dw",
        user="root",
        password=password,
    )
    cursor = conn.cursor(dictionary=True)

    queries = parse_queries()
    print(f"\nFound {len(queries)} queries in {QUERIES_FILE}.\n")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        out_f.write("=" * 80 + "\n")
        out_f.write("  RETAIL DATA WAREHOUSE — OLAP QUERY RESULTS (Q1 - Q20)\n")
        out_f.write("=" * 80 + "\n\n")

        for num, header, sql in queries:
            print("=" * 70)
            print(f"  EXECUTING QUERY Q{num}")
            print("=" * 70)
            print(header)
            print("-" * 70)

            out_f.write(f"\n{'=' * 70}\n  QUERY Q{num}\n{'=' * 70}\n")
            out_f.write(header + "\n\n")

            try:
                # Handle queries that might contain multiple statements (like Q20 CREATE VIEW + SELECT)
                statements = [s.strip() for s in sql.split(";") if s.strip()]
                for stmt in statements:
                    cursor.execute(stmt)
                    if cursor.description:
                        rows = cursor.fetchall()
                        df = pd.DataFrame(rows)
                        if df.empty:
                            print("(0 rows returned)")
                            out_f.write("(0 rows returned)\n")
                        else:
                            # Display first 20 rows nicely
                            pd.set_option("display.max_columns", None)
                            pd.set_option("display.width", 1000)
                            print(df.head(20).to_string(index=False))
                            if len(df) > 20:
                                print(f"\n... [{len(df)} total rows. First 20 displayed above]")
                            out_f.write(df.to_string(index=False) + f"\n\nTotal rows: {len(df)}\n")
                    else:
                        conn.commit()
                        print("Statement executed successfully.")
                        out_f.write("Statement executed successfully.\n")

            except Exception as e:
                print(f"[Error executing Q{num}]: {e}")
                out_f.write(f"[Error]: {e}\n")

            print("\n")

    cursor.close()
    conn.close()
    print(f">> All query outputs saved to: {OUTPUT_FILE}\n")


if __name__ == "__main__":
    pw = getpass.getpass("MySQL root password: ")
    run_all_queries(pw)
