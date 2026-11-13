# PROJECT_MEMORY.md

> Persistent memory file for the Retail Data Warehouse project.
> Updated automatically — do not edit manually unless correcting an error.

---

## 1. Project Identity and Scope

| Field       | Value |
|-------------|-------|
| **Title**   | Building and Analysing a Near-Real-Time and Periodic Data Warehouse for a Retail Chain |
| **Repository** | `retail-meshjoin-data-warehouse` (GitHub: `https://github.com/Izhan-Nasir/retail-meshjoin-data-warehouse.git`) |
| **Language** | Python 3.14.1 |
| **Database** | MySQL 8.0.46 |
| **Dataset**  | `Online Retail.xlsx` (22.6 MB, preserved untouched outside repo) |
| **Manual**   | `project markdown.md` |
| **Status**   | **Complete — All 10 Modules Implemented, Tested, Packaged & Pushed** ✅ |

---

## 2. Confirmed Requirements & Implementation Summary

1. **Dataset Cleaning (M3)** — 5-step automated cleaning pipeline: cancellations removed (9,288), Quantity ≤ 0 removed (1,336), UnitPrice ≤ 0 removed (1,181), missing CustomerID removed (132,220), exact duplicates removed (5,192). Raw: 541,909 → Cleaned: 392,692. Original file untouched.
2. **Database Setup & Schema (M2)** — Star schema with `FACT_SALES` + 5 dimensions: `DIM_CUSTOMER`, `DIM_PRODUCT`, `DIM_SUPPLIER`, `DIM_STORE`, `DIM_TIME`. Staging and working tables: `periodic_sales_data`, `shed_table`, `customer_master_data`, `product_master_data`, and view `STORE_QUARTERLY_SALES`.
3. **Master Data Generation (M4)** — Deterministic `stable()` function using MD5 % n. Customer MD: 4,338 records in 9 partitions. Product MD: 3,665 records in 8 partitions. Longest description used for product names.
4. **Data Preparation & Staging (M5)** — Invoice-level `sales_mode`: 0–6 online (275,154 rows written to `Data/online_sales_stream.csv`), 7–9 periodic (117,538 rows staged in `periodic_sales_data` across 48 release hours). Store assignment: Store 1 for online, Stores 2–5 for periodic.
5. **MESHJOIN Algorithm (M6)** — Interleaved dual-pass: loads 1 Customer partition (500 tuples) and 1 Product partition (500 tuples) per iteration via SQL `LIMIT/OFFSET`. Chunks persist for max(9, 8) = 9 iterations to guarantee full enrichment. Multi-map hash tables (`H_customer`, `H_product`) prevent key collisions. Master data never loaded into memory as a whole dictionary.
6. **Threading, Priority & Load-Shedding (M7)** — Three concurrent threads: `OnlineThread` (dynamic rate: 80 peak / 10 non-peak, stops at EOF), `PeriodicThread` (extracts 1 group/hour, marks released), `MeshJoinThread` (runs MESHJOIN iterations and coordinates shed recovery). Priority buffer: Online (P1) > Periodic (P2) > Shed-Recovered (P3). Load shedding triggers at buffer ≥ 800 (periodic only). Shed recovery triggers in non-peak when buffer < 500.
7. **Consolidation & Runtime CLI (M8)** — In-memory dimension key caching. Full temporal derivation for `DIM_TIME`. Enforces $revenue = quantity \times unit\_price$ and $processing\_mode = sales\_mode$. Interactive CLI supporting `online`, `periodic`, `both`, `peak`, `non-peak`, `status`, `quit`.
8. **OLAP Queries Q1–Q20 (M9)** — All 20 analytical queries written in `queries.sql`. Uses pre-computed `revenue`. Uses `LAG()` with NULL for first period in growth/volatility queries. Market basket analysis in Q16, `WITH ROLLUP` in Q17, spike detection in Q19, view creation in Q20.
9. **Packaging & Report (M10)** — Full technical report in `Project-Report/report.md`, operational manual in `ReadMe/README.md`, data provenance in `Data/data_notes.txt`. Final submission folders populated: `Create-DW/`, `Mesh-Join/`, `Queries-DW/`, `Data/`, `Project-Report/`, `ReadMe/`.

---

## 3. Human Decisions

| # | Question | Decision |
|---|----------|----------|
| Q1 | What is `processing_mode`? | `processing_mode` = `sales_mode` (same value in both columns) |
| Q2 | How does MESHJOIN handle two MD tables (Customer + Product)? | Interleaved dual-pass: each iteration loads one Customer partition AND one Product partition. Chunks stay for max(9, 8) = 9 iterations. |
| Q3 | What are the load-shedding thresholds? | Shed periodic transactions when Stream Buffer ≥ 800 of 1,000. Recover shed transactions in non-peak mode when buffer < 500. |
| Q4 | How do Q1 and Q11 differ? | Q1 = year-level top 5 products with drill-down by month. Q11 = independent top 5 per each calendar month. |
| Q5 | Handle 5,192 exact duplicate rows? | Remove all exact duplicates. Preserve original file. Record count removed. |
| Q6 | Keep special StockCodes (POST, M, BANK CHARGES, etc.)? | Keep them — manual cleaning rules don't mention StockCode filtering. 1,547 rows retained. |
| Q7 | Age group overlap at boundary 55 (51–55 vs 55+)? | Keep labels exactly as printed in the manual. `stable()` assigns unique index 0–6, no actual overlap. |
| Q8 | DIM_TIME granularity? | One row per unique (date, hour) combination. |
| Q9 | What does OnlineThread do at end of CSV? | Stop. Thread finishes, MESHJOIN drains remaining buffer. No replay. |
| Q10 | Q13 "sales contribution" definition? | Total revenue: `SUM(revenue)` per store → supplier → product. |
| Q11 | Q19 "average daily revenue" — include zero-sale days? | Average over days with actual sales only. |
| Q12 | Q20 materialized view or ordinary view? | Ordinary `CREATE VIEW`. MySQL has no native materialized views. Order by in retrieval query. |
| Q13 | Dataset download date? | November 6, 2026. |

---

## 4. Module Progress & Git Commits

| Module | Status | Unit Tests | Verification | Git Commit |
|--------|--------|------------|--------------|------------|
| M1: Repo Setup & Environment | **Complete** | 12/12 passed | ✅ | `2ec537a` |
| M2: DB Setup & Schema | **Complete** | 18/18 passed | ✅ | `0f4c17b` |
| M3: Data Cleaning | **Complete** | 21/21 passed | ✅ | `66749a6` |
| M4: Master Data Generation | **Complete** | 10/10 passed | ✅ | `726fcbe` |
| M5: Sales Mode, Store, Staging | **Complete** | 12/12 passed | ✅ | `40d5cfb` |
| M6: MESHJOIN Core Algorithm | **Complete** | 11/11 passed | ✅ | `3534079` |
| M7: Threading, Priority, Load Shedding | **Complete** | 9/9 passed | ✅ | `b5382a8` |
| M8: Runtime Commands & Consolidation | **Complete** | 7/7 passed | ✅ | `7a4ee84` |
| M9: OLAP Queries Q1–Q20 | **Complete** | 9/9 passed | ✅ | `10f3f43` |
| M10: Report, ReadMe, Final Packaging | **Complete** | Full packaging verified | ✅ | `4ecc6e2` |

---

## 5. Submission Folder Contents

1. **`Create-DW/`**:
   - `schema.sql`: Complete DDL script creating `FACT_SALES`, 5 dimension tables, working tables, and analytical views.
2. **`Mesh-Join/`**:
   - `config.py`: System configuration, thresholds, rates, connection helper.
   - `cleaning.py`: 5-step data cleaning module.
   - `master_data.py`: Deterministic customer and product master data generation.
   - `data_prep.py`: Sales mode assignment, store allocation, periodic staging.
   - `meshjoin.py`: MESHJOIN algorithm engine with interleaved dual-pass enrichment.
   - `shedding.py`: Load-shedding and recovery logic.
   - `threads.py`: `OnlineThread`, `PeriodicThread`, `MeshJoinThread`.
   - `consolidation.py`: Dimension upsert, temporal derivation, `FACT_SALES` loading.
   - `runtime.py`: Main CLI runtime application.
   - `db_setup.py`: Database table creation script.
3. **`Queries-DW/`**:
   - `queries.sql`: Complete SQL script containing all 20 analytical queries (Q1–Q20).
4. **`Data/`**:
   - `data_notes.txt`: Data provenance, cleaning audit trail, dataset citation.
   - `customer_master_data.csv`: Generated customer master data (4,338 rows).
   - `product_master_data.csv`: Generated product master data (3,665 rows).
   - `online_sales_stream.csv`: Online transactional stream (275,154 rows).
5. **`Project-Report/`**:
   - `report.md`: Formal academic project report including architecture, MESHJOIN mechanics, 3 shortcomings, and audit metrics.
6. **`ReadMe/`**:
   - `README.md`: Execution guide, installation steps, runtime commands, and test instructions.
