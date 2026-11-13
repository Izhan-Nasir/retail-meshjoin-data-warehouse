"""
run_queries.py — Top-level launcher.
Delegates execution to Queries-DW/run_queries.py.
"""
import pathlib
import runpy
import sys

_QUERIES_DIR = pathlib.Path(__file__).resolve().parent / "Queries-DW"
if str(_QUERIES_DIR) not in sys.path:
    sys.path.insert(0, str(_QUERIES_DIR))

if __name__ == "__main__":
    runpy.run_path(str(_QUERIES_DIR / "run_queries.py"), run_name="__main__")
