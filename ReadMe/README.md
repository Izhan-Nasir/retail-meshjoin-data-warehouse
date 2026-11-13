# Retail Chain Near-Real-Time & Periodic Data Warehouse

A university practical project implementing a near-real-time streaming and periodic Data Warehouse for a multinational retail chain using **Python 3.14**, **MySQL 8.0**, and the **MESHJOIN (Mesh Join)** stream-table join algorithm.

---

## 1. System Architecture

The solution implements a hybrid near-real-time and periodic data warehouse architecture:

```
[Online Stream CSV] ──────> OnlineThread (Priority 1) ───┐
                                                         │
[Periodic Staging DB] ────> PeriodicThread (Priority 2) ──┼──> [StreamBuffer (Cap: 1000)]
                                                         │        │
[Shed Table Recovery] ────> Priority 3 ──────────────────┘        ▼
                                                              [MESHJOIN Engine]
                                                                  ├── Customer MD (9 partitions)
                                                                  └── Product MD  (8 partitions)
                                                                  │
                                                                  ▼
                                                              [DataConsolidator]
                                                                  │
                                                                  ▼
                                                              [FACT_SALES & DIM Tables]
```

### Key Components:
1. **Star Schema (`schema.sql`):**
   - Central Fact: `FACT_SALES` (one row per product per shopping cart)
   - 5 Dimensions: `DIM_CUSTOMER`, `DIM_PRODUCT`, `DIM_SUPPLIER`, `DIM_STORE`, `DIM_TIME`
   - Auxiliary Tables: `periodic_sales_data`, `shed_table`, `customer_master_data`, `product_master_data`
   - Analytical View: `STORE_QUARTERLY_SALES`
2. **Deterministic Master Data Generation (`master_data.py`):**
   - Implements portable `stable(key, n)` using 8-character MD5 hex slice mod $n$.
   - Customer MD: 4,338 records divided into 9 partitions (500 tuples/partition).
   - Product MD: 3,665 records divided into 8 partitions (500 tuples/partition).
3. **Data Preparation & Split (`data_prep.py`):**
   - Invoice-level sales-mode assignment: `MD5(invoice_no) % 10`
   - 70.1% online stream (275,154 rows written to `Data/online_sales_stream.csv`).
   - 29.9% periodic batch (117,538 rows staged into `periodic_sales_data` across 48 simulated hours).
4. **MESHJOIN Core Engine (`meshjoin.py`):**
   - Dual-pass interleaved join: loads 1 Customer partition and 1 Product partition each iteration.
   - Multi-map hash tables: `H_customer` and `H_product`.
   - Age-based chunk eviction: Chunks evict after $\max(9, 8) = 9$ iterations, guaranteeing 100% complete enrichment.
   - Master Data is read in 500-tuple slices via MySQL `LIMIT 500 OFFSET offset`; the full table is never loaded into memory.
5. **Adaptive Load Shedding & Multi-Threading (`threads.py`, `shedding.py`):**
   - Three independent concurrent threads: `OnlineThread`, `PeriodicThread`, `MeshJoinThread`.
   - Priority buffer: Online (P1) > Periodic (P2) > Shed-Recovered (P3).
   - Load shedding triggers when buffer usage $\ge 800$; periodic transactions are diverted to `shed_table`.
   - Recovery triggers in non-peak mode when buffer usage $< 500$.
6. **Data Consolidation (`consolidation.py`):**
   - Derives full temporal hierarchy (date, hour, month, quarter, year, weekday/weekend, season, half-year).
   - In-memory dimension key caching for high-throughput fact insertion.
   - Guarantees $revenue = quantity \times unit\_price$ and $processing\_mode = sales\_mode$.
7. **OLAP Queries (`queries.sql`):**
   - 20 complete analytical SQL queries addressing customer demographics, product ranking, growth percentages, cross-selling, rollups, and spike detection.

---

## 2. Prerequisites & Installation

### Environment Requirements:
- **Operating System:** Windows, Linux, or macOS
- **Python:** Version 3.14+ (or 3.10+)
- **MySQL:** Version 8.0+ running on `localhost:3306`

### Python Package Installation:
```powershell
pip install -r requirements.txt
```
*(Dependencies: `mysql-connector-python`, `openpyxl`, `pandas`, `pytest`)*

---

## 3. Step-by-Step Execution Guide

### Step 1: Database Setup
Create all star schema tables, working staging tables, and views:
```powershell
py -3.14 db_setup.py
```
*(Alternatively: `mysql -u root -p < schema.sql`)*

### Step 2: Data Cleaning & Validation
Execute the 5-step data cleaning pipeline on `Online Retail.xlsx`:
```powershell
py -3.14 cleaning.py
```
Outputs cleaning metrics and validates that exact duplicates (5,192 rows) and invalid entries are purged.

### Step 3: Master Data Generation
Generate deterministic customer and product master data, export CSVs, and load MySQL tables:
```powershell
py -3.14 master_data.py
```

### Step 4: Data Preparation & Periodic Staging
Partition cleaned data into online CSV and stage 48 periodic groups into MySQL:
```powershell
py -3.14 data_prep.py
```

### Step 5: Start Runtime Engine
Launch the multi-threaded ETL pipeline with interactive command interface:
```powershell
py -3.14 runtime.py
```

### Interactive Runtime Commands:
| Command | Action |
|---|---|
| `online` | Streams online transactions only (pauses periodic extraction) |
| `periodic` | Extracts periodic hourly batches only (pauses online stream) |
| `both` | Runs both streams concurrently with Online Priority |
| `peak` | Sets stream rate to 80 tx/sec and activates load shedding |
| `non-peak` | Sets stream rate to 10 tx/sec and activates shed recovery |
| `status` | Displays real-time buffer count, hash entries, iterations, and DW fact counts |
| `quit` | Gracefully shuts down all threads, joins workers, and closes DB connection |

### Step 6: Execute OLAP Analytical Queries
Run the 20 analytical queries against the populated Data Warehouse:
```powershell
mysql -u root -p retail_dw < queries.sql
```

---

## 4. Running the Automated Test Suite

To run all unit tests across all modules (fast execution, synthetic fixtures):
```powershell
py -3.14 -m pytest tests/ -v
```

---

## 5. Submission Folder Structure

```
retail-meshjoin-data-warehouse/
├── Create-DW/
│   └── schema.sql                # Complete DDL script
├── Mesh-Join/
│   ├── config.py                 # System configuration and connection helper
│   ├── cleaning.py               # 5-step data cleaning module
│   ├── master_data.py            # Master data generator
│   ├── data_prep.py              # Sales-mode assignment and periodic staging
│   ├── meshjoin.py               # MESHJOIN stream-table join engine
│   ├── shedding.py               # Load shedding and recovery module
│   ├── threads.py                # Online, Periodic, and MeshJoin worker threads
│   ├── consolidation.py          # Dimension upsert and FACT_SALES loading
│   └── runtime.py                # Main runtime application and CLI
├── Queries-DW/
│   └── queries.sql               # 20 OLAP queries (Q1 - Q20)
├── Data/
│   ├── customer_master_data.csv  # 4,338 customer records
│   ├── product_master_data.csv   # 3,665 product records
│   ├── online_sales_stream.csv   # 275,154 online transactions
│   └── data_notes.txt            # Dataset provenance, cleaning audit, citations
├── Project-Report/
│   └── report.md                 # Full project report and technical documentation
└── ReadMe/
    └── README.md                 # Project execution manual
```

---

## 6. Project Provenance & Citations

- **Dataset:** Online Retail Dataset, UCI Machine Learning Repository
- **Citation:** Chen, D. (2015). Online Retail [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33
- **Download Date:** November 6, 2026
- **Direct Fact Loading:** Direct batch fact loading used. Fact caching bonus not implemented.
