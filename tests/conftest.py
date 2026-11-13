import pathlib
import sys

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

for _p in [
    _REPO_ROOT / "Mesh-Join",
    _REPO_ROOT / "Create-DW",
    _REPO_ROOT / "Queries-DW",
]:
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
