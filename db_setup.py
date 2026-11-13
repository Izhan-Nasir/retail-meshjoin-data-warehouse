"""
db_setup.py — Top-level launcher.
Delegates execution to Create-DW/db_setup.py.
"""
import pathlib
import runpy
import sys

_CREATE_DIR = pathlib.Path(__file__).resolve().parent / "Create-DW"
if str(_CREATE_DIR) not in sys.path:
    sys.path.insert(0, str(_CREATE_DIR))

if __name__ == "__main__":
    runpy.run_path(str(_CREATE_DIR / "db_setup.py"), run_name="__main__")
