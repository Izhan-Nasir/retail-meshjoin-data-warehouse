"""
tests/test_m9_queries.py
Module 9 unit tests — fast static & semantic verification of queries.sql.
No slow Excel loading.
"""

import os
import re
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_Q1 = os.path.join(os.path.dirname(__file__), "..", "Queries-DW", "queries.sql")
_Q2 = os.path.join(os.path.dirname(__file__), "..", "queries.sql")
QUERIES_FILE = _Q1 if os.path.exists(_Q1) else _Q2


@pytest.fixture(scope="module")
def sql_content():
    assert os.path.exists(QUERIES_FILE), "queries.sql must exist"
    with open(QUERIES_FILE, "r", encoding="utf-8") as f:
        return f.read()


def test_queries_file_exists(sql_content):
    """queries.sql must exist and have substantial content."""
    assert len(sql_content) > 1000


def test_all_20_queries_present(sql_content):
    """All 20 queries (Q1 to Q20) must be clearly delineated with comments."""
    for i in range(1, 21):
        pattern = rf"--\s*Q{i}:"
        assert re.search(pattern, sql_content), f"Query Q{i} header comment missing"


def test_stored_revenue_used_not_recomputed(sql_content):
    """Queries must use the stored `revenue` column, NOT compute quantity * price."""
    # Ensure no expression like quantity * price or quantity * unit_price is used for revenue
    bad_calc = re.search(r"quantity\s*\*\s*(?:unit_)?price", sql_content, re.IGNORECASE)
    assert bad_calc is None, f"Found prohibited on-the-fly revenue calculation: {bad_calc.group(0)}"


def test_q1_and_q11_are_distinct(sql_content):
    """Q1 (year-level top 5) and Q11 (monthly top 5) must be distinct queries."""
    q1_match = re.search(r"--\s*Q1:.*?(?=--\s*Q2:)", sql_content, re.DOTALL)
    q11_match = re.search(r"--\s*Q11:.*?(?=--\s*Q12:)", sql_content, re.DOTALL)
    assert q1_match and q11_match
    q1_text = q1_match.group(0)
    q11_text = q11_match.group(0)
    assert q1_text != q11_text
    assert "LIMIT 5" in q1_text  # Year-level top 5
    assert "PARTITION BY t.year, t.month" in q11_text  # Monthly top 5


def test_growth_and_volatility_use_lag(sql_content):
    """Q9, Q12, and Q15 must utilize LAG() window function."""
    for q in ["Q9", "Q12", "Q15"]:
        pattern = rf"--\s*{q}:.*?(?=--\s*Q\d+:|$)"
        match = re.search(pattern, sql_content, re.DOTALL)
        assert match, f"{q} not found"
        assert "LAG(" in match.group(0).upper(), f"{q} does not use LAG() window function"


def test_q16_self_join_distinct_cart(sql_content):
    """Q16 must self-join on transaction_id and use COUNT(DISTINCT transaction_id)."""
    match = re.search(r"--\s*Q16:.*?(?=--\s*Q17:)", sql_content, re.DOTALL)
    assert match
    text = match.group(0)
    assert "JOIN FACT_SALES" in text
    assert "product_key <" in text
    assert "COUNT(DISTINCT" in text


def test_q17_with_rollup(sql_content):
    """Q17 must use WITH ROLLUP for multi-level hierarchical aggregation."""
    match = re.search(r"--\s*Q17:.*?(?=--\s*Q18:)", sql_content, re.DOTALL)
    assert match
    assert "WITH ROLLUP" in match.group(0).upper()


def test_q19_spike_detection_cte(sql_content):
    """Q19 must identify spikes > 2x average daily revenue."""
    match = re.search(r"--\s*Q19:.*?(?=--\s*Q20:)", sql_content, re.DOTALL)
    assert match
    text = match.group(0)
    assert "2.0 *" in text or "2 *" in text


def test_q20_create_view_and_order_outside(sql_content):
    """Q20 must CREATE VIEW without ORDER BY inside, and ORDER BY in subsequent SELECT."""
    match = re.search(r"--\s*Q20:.*", sql_content, re.DOTALL)
    assert match
    text = match.group(0)
    assert "CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES" in text
    # View definition should not contain ORDER BY
    view_def = text[:text.find(";")]
    assert "ORDER BY" not in view_def.upper()
    # Retrieval query should contain ORDER BY
    retrieval = text[text.find(";"):]
    assert "ORDER BY" in retrieval.upper()

