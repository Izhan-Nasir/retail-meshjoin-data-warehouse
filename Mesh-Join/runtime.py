"""
runtime.py — Main entry point for the Retail MESHJOIN Data Warehouse system.

Runs the near-real-time streaming and periodic ETL pipeline:
- Starts OnlineThread, PeriodicThread, and MeshJoinThread.
- Interactive CLI accepts 7 commands:
    online    — enable online stream only
    periodic  — enable periodic extraction only
    both      — enable both streams (online has priority)
    peak      — set rate to 80 tx/sec and enable load shedding
    non-peak  — set rate to 10 tx/sec and enable shed recovery
    status    — print current system metrics
    quit      — gracefully shut down all threads and exit
"""

from __future__ import annotations

import getpass
import os
import sys
import threading
import time

from config import (
    NON_PEAK_RATE,
    ONLINE_CSV,
    PEAK_RATE,
    SIMULATED_HOUR_SECONDS,
    STREAM_BUFFER_CAPACITY,
    get_connection,
)
from consolidation import DataConsolidator
from meshjoin import MeshJoinEngine, StreamBuffer, create_db_loaders
from shedding import get_shed_count
from threads import MeshJoinThread, OnlineThread, PeriodicThread


def print_banner(mode: str, is_peak: bool, online_csv: str) -> None:
    rate = PEAK_RATE if is_peak else NON_PEAK_RATE
    print("=" * 70)
    print("  RETAIL MESHJOIN DATA WAREHOUSE — RUNTIME ENGINE")
    print("=" * 70)
    print(f"  Online Data Source       : {online_csv}")
    print(f"  Arrival Mode             : {'PEAK' if is_peak else 'NON-PEAK'}")
    print(f"  Stream Arrival Rate      : {rate} tx/sec")
    print(f"  Stream Buffer Capacity   : {STREAM_BUFFER_CAPACITY} transactions")
    print(f"  Simulated Hour Duration  : {SIMULATED_HOUR_SECONDS} seconds")
    print(f"  Peak Rate / Non-Peak Rate: {PEAK_RATE} tx/s / {NON_PEAK_RATE} tx/s")
    print(f"  Active Stream Mode       : {mode.upper()}")
    print("-" * 70)
    print("Commands: [online | periodic | both | peak | non-peak | status | quit]")
    print("=" * 70)


def print_status(
    stream_buffer: StreamBuffer,
    engine: MeshJoinEngine,
    online_thread: OnlineThread,
    periodic_thread: PeriodicThread,
    meshjoin_thread: MeshJoinThread,
    is_peak: bool,
    mode: str,
    conn=None,
) -> None:
    stats = engine.get_stats()
    shed_cnt = get_shed_count(conn=conn)
    cur = conn.cursor() if conn else None
    fact_cnt = 0
    if cur:
        try:
            conn.commit()
        except Exception:
            pass
        cur.execute("SELECT COUNT(*) FROM FACT_SALES")
        fact_cnt = cur.fetchone()[0]
        cur.close()

    print("\n" + "-" * 50)
    print("  SYSTEM STATUS REPORT")
    print("-" * 50)
    print(f"  Active Mode            : {mode.upper()}")
    print(f"  Arrival Mode           : {'PEAK' if is_peak else 'NON-PEAK'}")
    print(f"  Current Online Rate    : {online_thread.get_rate()} tx/sec")
    print(f"  Stream Buffer Usage    : {stream_buffer.size()} / {stream_buffer.capacity}")
    print(f"  Hash Table Entries     : {stats['h_customer_keys']} customers, {stats['h_product_keys']} products")
    print(f"  Active Chunks in Queue : {stats['queue_chunks']}")
    print(f"  MESHJOIN Iterations    : {stats['iterations']}")
    print(f"  Periodic Groups Loaded : {periodic_thread.groups_released}")
    print(f"  Shed Disk Table Count  : {shed_cnt}")
    print(f"  Recovered Transactions : {meshjoin_thread.total_recovered}")
    print(f"  FACT_SALES Rows in DW  : {fact_cnt}")
    print("-" * 50 + "\n")


def run(
    host: str = "localhost",
    port: int = 3306,
    database: str = "retail_dw",
    user: str = "root",
    password: str = "",
    online_csv: str = ONLINE_CSV,
    auto_quit_on_eof: bool = False,
) -> None:
    # 1. Connect to Database (separate connection per thread for safe concurrency)
    print(f"Connecting to MySQL at {host}:{port}/{database} ...")
    conn_main = get_connection(host, port, database, user, password)
    conn_main.autocommit = True
    conn_meshjoin = get_connection(host, port, database, user, password)
    conn_periodic = get_connection(host, port, database, user, password)
    print("Connected successfully.")

    # 2. Initialize Components
    stream_buffer = StreamBuffer(capacity=STREAM_BUFFER_CAPACITY)
    c_loader, p_loader, n_cust_parts, n_prod_parts = create_db_loaders(conn_meshjoin)
    engine = MeshJoinEngine(
        stream_buffer=stream_buffer,
        customer_loader=c_loader,
        product_loader=p_loader,
        num_customer_partitions=n_cust_parts,
        num_product_partitions=n_prod_parts,
    )
    consolidator = DataConsolidator(conn_meshjoin)

    # 3. Thread Synchronization Events & State
    stop_event = threading.Event()
    online_enabled = threading.Event()
    online_enabled.set()
    periodic_enabled = threading.Event()
    periodic_enabled.set()

    is_peak = True
    mode = "both"

    def on_enriched_output(batch):
        consolidator.consolidate_batch(batch)

    # 4. Instantiate Threads
    online_thread = OnlineThread(
        stream_buffer=stream_buffer,
        csv_path=online_csv,
        rate=PEAK_RATE,
        enabled_event=online_enabled,
        stop_event=stop_event,
    )
    periodic_thread = PeriodicThread(
        stream_buffer=stream_buffer,
        conn=conn_periodic,
        simulated_hour_seconds=SIMULATED_HOUR_SECONDS,
        enabled_event=periodic_enabled,
        stop_event=stop_event,
    )
    meshjoin_thread = MeshJoinThread(
        engine=engine,
        output_callback=on_enriched_output,
        conn=conn_meshjoin,
        is_peak_func=lambda: is_peak,
        stop_event=stop_event,
    )

    # 5. Start Threads
    online_thread.start()
    periodic_thread.start()
    meshjoin_thread.start()

    print_banner(mode, is_peak, online_csv)

    # 6. Command Loop
    try:
        while not stop_event.is_set():
            if auto_quit_on_eof and online_thread.is_eof and periodic_thread.is_finished and engine.is_finished():
                print("[Runtime] All streams and queues drained. Exiting...")
                break

            try:
                cmd = input().strip().lower()
            except (EOFError, KeyboardInterrupt):
                cmd = "quit"

            if cmd == "online":
                periodic_enabled.clear()
                online_enabled.set()
                mode = "online"
                print(">> Switched to ONLINE-only mode.")

            elif cmd == "periodic":
                online_enabled.clear()
                periodic_enabled.set()
                mode = "periodic"
                print(">> Switched to PERIODIC-only mode.")

            elif cmd == "both":
                online_enabled.set()
                periodic_enabled.set()
                mode = "both"
                print(">> Switched to BOTH mode (online priority).")

            elif cmd == "peak":
                is_peak = True
                online_thread.set_rate(PEAK_RATE)
                print(f">> Mode set to PEAK ({PEAK_RATE} tx/sec). Load shedding active.")

            elif cmd == "non-peak":
                is_peak = False
                online_thread.set_rate(NON_PEAK_RATE)
                print(f">> Mode set to NON-PEAK ({NON_PEAK_RATE} tx/sec). Shed recovery active.")

            elif cmd == "status":
                print_status(
                    stream_buffer, engine, online_thread, periodic_thread,
                    meshjoin_thread, is_peak, mode, conn=conn_main
                )

            elif cmd == "quit":
                print(">> Initiating shutdown...")
                stop_event.set()
                break

            elif cmd == "":
                continue

            else:
                print(f"Unknown command: '{cmd}'. Available: online, periodic, both, peak, non-peak, status, quit")

    finally:
        stop_event.set()
        print("Waiting for worker threads to stop ...")
        online_thread.join(timeout=3.0)
        periodic_thread.join(timeout=3.0)
        meshjoin_thread.join(timeout=3.0)
        conn_main.close()
        conn_meshjoin.close()
        conn_periodic.close()
        print("Database connection closed. Runtime terminated cleanly.")


if __name__ == "__main__":
    print("=" * 60)
    print("  RETAIL MESHJOIN DATA WAREHOUSE — CONFIGURATION PROMPTS")
    print("=" * 60)
    h = input("MySQL host [localhost]: ").strip() or "localhost"
    p_str = input("MySQL port [3306]: ").strip() or "3306"
    p = int(p_str)
    d = input("Database name [retail_dw]: ").strip() or "retail_dw"
    u = input("Username [root]: ").strip() or "root"
    pw = getpass.getpass("Password: ")
    csv_file = input(f"Online CSV path [{ONLINE_CSV}]: ").strip() or ONLINE_CSV

    run(host=h, port=p, database=d, user=u, password=pw, online_csv=csv_file)
