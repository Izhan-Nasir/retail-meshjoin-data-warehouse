"""
config.py — Project-wide constants and DB connection helper.

Do NOT hardcode passwords here. DB credentials are entered at runtime.
"""

# ── MESHJOIN Parameters ───────────────────────────────────────────────────────
PARTITION_SIZE = 500          # Tuples per Master Data partition
STREAM_CHUNK_SIZE = 500       # Transactions read from Stream Buffer per iteration
STREAM_BUFFER_CAPACITY = 1000 # Max transactions in the Stream Buffer

# ── Threading / Timing ───────────────────────────────────────────────────────
SIMULATED_HOUR_SECONDS = 60   # 60 real seconds = 1 simulated hour
PEAK_RATE = 80                # Online transactions per second in peak mode
NON_PEAK_RATE = 10            # Online transactions per second in non-peak mode

# ── Load-Shedding Thresholds ─────────────────────────────────────────────────
SHED_THRESHOLD = 800          # Shed periodic if Stream Buffer >= this
RECOVERY_THRESHOLD = 500      # Recover shed txns if buffer < this (non-peak)

# ── Periodic Groups ───────────────────────────────────────────────────────────
NUM_PERIODIC_GROUPS = 48

# ── Store List (in-memory, not via MESHJOIN) ──────────────────────────────────
STORE_LIST = {
    1: {"store_name": "Online",               "city": "Web"},
    2: {"store_name": "London Bridge",        "city": "London"},
    3: {"store_name": "Manchester Arndale",   "city": "Manchester"},
    4: {"store_name": "Birmingham Bullring",  "city": "Birmingham"},
    5: {"store_name": "Edinburgh Princes",    "city": "Edinburgh"},
}

# ── File Paths ────────────────────────────────────────────────────────────────
# Default dataset location (relative to project root — not inside the repo)
DATASET_PATH = r"D:\University Semesters\7th Semester\DWHBI\Project\Online Retail.xlsx"

DATA_DIR = "Data"
ONLINE_CSV = "Data/online_sales_stream.csv"
CUSTOMER_MD_CSV = "Data/customer_master_data.csv"
PRODUCT_MD_CSV = "Data/product_master_data.csv"


# ── DB Connection Helper ──────────────────────────────────────────────────────
def get_connection(host, port, database, user, password):
    """Return a live mysql.connector connection. Raises on failure."""
    import mysql.connector
    return mysql.connector.connect(
        host=host,
        port=int(port),
        database=database,
        user=user,
        password=password,
        autocommit=False,
    )

