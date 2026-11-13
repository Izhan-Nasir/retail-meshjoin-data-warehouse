"""
runtime.py — Top-level launcher.
Delegates execution to Mesh-Join/runtime.py.
"""
import pathlib
import runpy
import sys

_MESH_JOIN = pathlib.Path(__file__).resolve().parent / "Mesh-Join"
if str(_MESH_JOIN) not in sys.path:
    sys.path.insert(0, str(_MESH_JOIN))

if __name__ == "__main__":
    runpy.run_path(str(_MESH_JOIN / "runtime.py"), run_name="__main__")
