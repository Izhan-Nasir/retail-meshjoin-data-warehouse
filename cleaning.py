"""
cleaning.py — Read the raw Online Retail dataset, apply all cleaning rules,
remove exact duplicates, and return a clean pandas DataFrame.

Cleaning order (per project manual §3 and human decision Q5):
  1. Remove cancellations (transaction_id starts with 'C')
  2. Remove rows where quantity <= 0
  3. Remove rows where unit_price <= 0
  4. Remove rows where customer_id is null
  5. Remove exact duplicate rows

Counts at each step are printed to stdout and must appear in the report.
The original file is NEVER modified.
"""
# Implementation in Module 3

