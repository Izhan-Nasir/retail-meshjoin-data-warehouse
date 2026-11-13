"""
tests/test_m1_setup.py — Module 1 acceptance tests.
Verifies: imports, MySQL connectivity, project skeleton, .gitignore.
"""
import importlib
import os
import pathlib

REPO = pathlib.Path(__file__).parent.parent  # repo root


def test_mysql_connector_importable():
    import mysql.connector
    assert mysql.connector.__version__


def test_openpyxl_importable():
    import openpyxl
    assert openpyxl.__version__


def test_config_importable():
    import config
    assert config.PARTITION_SIZE == 500
    assert config.STREAM_CHUNK_SIZE == 500
    assert config.STREAM_BUFFER_CAPACITY == 1000
    assert config.PEAK_RATE == 80
    assert config.NON_PEAK_RATE == 10
    assert config.SIMULATED_HOUR_SECONDS == 60
    assert config.SHED_THRESHOLD == 800
    assert config.RECOVERY_THRESHOLD == 500
    assert config.NUM_PERIODIC_GROUPS == 48
    assert len(config.STORE_LIST) == 5


def test_store_list_values():
    import config
    stores = config.STORE_LIST
    assert stores[1]["store_name"] == "Online"
    assert stores[2]["store_name"] == "London Bridge"
    assert stores[3]["store_name"] == "Manchester Arndale"
    assert stores[4]["store_name"] == "Birmingham Bullring"
    assert stores[5]["store_name"] == "Edinburgh Princes"


def test_mysql_connection():
    import mysql.connector
    conn = mysql.connector.connect(
        host="localhost", port=3306, user="root", password="8956"
    )
    assert conn.is_connected()
    conn.close()


def test_gitignore_excludes_xlsx():
    gi = REPO / ".gitignore"
    assert gi.exists()
    content = gi.read_text()
    assert "*.xlsx" in content


def test_gitignore_excludes_env():
    gi = REPO / ".gitignore"
    content = gi.read_text()
    assert ".env" in content


def test_gitignore_excludes_pycache():
    gi = REPO / ".gitignore"
    content = gi.read_text()
    assert "__pycache__" in content


def test_submission_folders_exist():
    for folder in ["Create-DW", "Mesh-Join", "Queries-DW", "Data", "Project-Report", "ReadMe", "docs", "tests"]:
        assert (REPO / folder).is_dir(), f"Missing folder: {folder}"


def test_source_stubs_exist():
    for f in ["config.py", "cleaning.py", "master_data.py", "data_prep.py",
              "db_setup.py", "meshjoin.py", "threads.py", "shedding.py",
              "consolidation.py", "runtime.py"]:
        assert (REPO / f).exists(), f"Missing stub: {f}"


def test_requirements_txt_exists():
    req = REPO / "requirements.txt"
    assert req.exists()
    content = req.read_text()
    assert "mysql-connector-python" in content
    assert "pytest" in content
    assert "openpyxl" in content


def test_dataset_original_location():
    """Original dataset must still exist in its original location."""
    dataset = pathlib.Path(
        r"D:\University Semesters\7th Semester\DWHBI\Project\Online Retail.xlsx"
    )
    assert dataset.exists(), "Original dataset missing!"
    # Must NOT be inside the repo
    assert not str(dataset).startswith(str(REPO)), "Dataset must not be inside repo"
