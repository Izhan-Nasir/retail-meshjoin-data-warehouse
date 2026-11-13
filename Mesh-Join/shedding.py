"""
shedding.py — Load-shedding logic and shed_table management.

Rules:
1. Shed Trigger: Stream Buffer size >= SHED_THRESHOLD (800) AND incoming transaction is periodic.
   Online transactions are NEVER shed.
2. Shed Action: Insert shed transactions into MySQL `shed_table` with shed_time=NOW() and flag='periodic'.
3. Partial Shedding: If buffer has space for some tuples, accept up to capacity and shed the remainder.
4. Recovery Trigger: System is in non-peak mode AND Stream Buffer size < RECOVERY_THRESHOLD (500).
5. Recovery Action: Read shed transactions from `shed_table`, push them into StreamBuffer with priority 3,
   and delete them from `shed_table`.
"""

from __future__ import annotations

import datetime
from typing import Any, List, Optional, Tuple

from config import (
    RECOVERY_THRESHOLD,
    SHED_THRESHOLD,
    STREAM_BUFFER_CAPACITY,
)
from meshjoin import StreamBuffer, Transaction


def should_shed(buffer_size: int, incoming_is_periodic: bool) -> bool:
    """Return True if load shedding should trigger for incoming periodic data."""
    return incoming_is_periodic and (buffer_size >= SHED_THRESHOLD)


def should_recover(buffer_size: int, is_peak: bool) -> bool:
    """Return True if shed recovery should trigger."""
    return (not is_peak) and (buffer_size < RECOVERY_THRESHOLD)


def shed_transactions(
    txns: List[Transaction],
    conn=None,
    in_memory_sink: Optional[List[Transaction]] = None,
) -> int:
    """
    Persist shed transactions into shed_table.
    If conn is provided, executes bulk INSERT into MySQL.
    If in_memory_sink is provided, appends transactions to the list (useful for testing/fallback).
    Returns count of transactions shed.
    """
    if not txns:
        return 0

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if conn is not None:
        cur = conn.cursor()
        sql = """
            INSERT INTO shed_table
                (transaction_id, customer_id, product_id, quantity, unit_price,
                 event_time, sales_mode, shed_time, flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                str(t.transaction_id),
                str(t.customer_id),
                str(t.product_id),
                int(t.quantity),
                float(t.unit_price),
                t.event_time if isinstance(t.event_time, str) else t.event_time.strftime("%Y-%m-%d %H:%M:%S"),
                str(t.sales_mode),
                now,
                "periodic",
            )
            for t in txns
        ]
        cur.executemany(sql, rows)
        conn.commit()
        cur.close()

    if in_memory_sink is not None:
        in_memory_sink.extend(txns)

    print(f"[LOAD SHED] Shed {len(txns)} periodic transactions to shed_table.")
    return len(txns)


def recover_shed_transactions(
    max_count: int,
    conn=None,
    in_memory_sink: Optional[List[Transaction]] = None,
) -> List[Transaction]:
    """
    Retrieve up to max_count transactions from shed_table, delete them from the table,
    and return them as Transaction objects ready to enter StreamBuffer at priority 3.
    """
    if max_count <= 0:
        return []

    recovered: List[Transaction] = []

    if conn is not None:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, transaction_id, customer_id, product_id, quantity, unit_price,
                   event_time, sales_mode
            FROM shed_table
            ORDER BY id ASC
            LIMIT %s
            """,
            (max_count,),
        )
        rows = cur.fetchall()

        if rows:
            ids_to_delete = [r["id"] for r in rows]
            format_strings = ",".join(["%s"] * len(ids_to_delete))
            cur.execute(
                f"DELETE FROM shed_table WHERE id IN ({format_strings})",
                tuple(ids_to_delete),
            )
            conn.commit()

            for r in rows:
                t = Transaction(
                    transaction_id=str(r["transaction_id"]),
                    customer_id=str(r["customer_id"]),
                    product_id=str(r["product_id"]),
                    quantity=int(r["quantity"]),
                    unit_price=float(r["unit_price"]),
                    event_time=r["event_time"],
                    sales_mode=r["sales_mode"],
                    processing_mode=r["sales_mode"],
                )
                recovered.append(t)
        cur.close()

    elif in_memory_sink is not None:
        count = min(max_count, len(in_memory_sink))
        for _ in range(count):
            recovered.append(in_memory_sink.pop(0))

    if recovered:
        print(f"[SHED RECOVERY] Recovered {len(recovered)} transactions from shed_table.")

    return recovered


def get_shed_count(conn=None, in_memory_sink: Optional[List[Transaction]] = None) -> int:
    """Return the current number of shed transactions awaiting recovery."""
    if conn is not None:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM shed_table")
        cnt = cur.fetchone()[0]
        cur.close()
        return cnt
    elif in_memory_sink is not None:
        return len(in_memory_sink)
    return 0
