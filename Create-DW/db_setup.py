"""
db_setup.py — Execute schema.sql against MySQL to create all tables and the view.

Usage:
    py -3.14 db_setup.py

Will prompt for DB credentials if not supplied as arguments.
Safe to re-run: drops and recreates everything.
"""
import sys
import os
import pathlib
import mysql.connector


SCHEMA_FILE = pathlib.Path(__file__).parent / "schema.sql"


def run_schema(host, port, database, user, password):
    """Execute schema.sql against the given database."""
    print(f"[db_setup] Connecting to MySQL at {host}:{port} ...")
    conn = mysql.connector.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        autocommit=True,
    )
    cursor = conn.cursor()

    # Create database if it doesn't exist
    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{database}` "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    cursor.execute(f"USE `{database}`")
    print(f"[db_setup] Using database: {database}")

    # Read and split SQL script on semicolons
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    created = []
    for stmt in statements:
        # Skip fragments that are only comments or whitespace
        non_comment = "\n".join(
            line for line in stmt.splitlines()
            if not line.strip().startswith("--")
        ).strip()
        if not non_comment:
            continue
        try:
            cursor.execute(stmt)
            # Extract the object name for a friendly log message
            first_line = non_comment.split("\n")[0].strip()
            created.append(first_line[:80])
            print(f"  OK  {first_line[:80]}")
        except mysql.connector.Error as err:
            print(f"  ERR {non_comment[:80]}\n      → {err}", file=sys.stderr)
            cursor.close()
            conn.close()
            sys.exit(1)

    cursor.close()
    conn.close()
    print(f"\n[db_setup] Done — {len(created)} statements executed successfully.")


def _prompt_credentials():
    host     = input("MySQL host     [localhost]: ").strip() or "localhost"
    port     = input("MySQL port     [3306]:     ").strip() or "3306"
    database = input("Database name  [retail_dw]: ").strip() or "retail_dw"
    user     = input("MySQL user     [root]:     ").strip() or "root"
    import getpass
    if sys.stdin.isatty():
        password = getpass.getpass("MySQL password: ")
    else:
        password = sys.stdin.readline().strip()
    return host, port, database, user, password


if __name__ == "__main__":
    creds = _prompt_credentials()
    run_schema(*creds)
