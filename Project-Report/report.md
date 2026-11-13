# Building and Analysing a Near-Real-Time and Periodic Data Warehouse for a Retail Chain

**Student Name:** Izhan Nasir  
**Roll Number:** 23i-2517  
**Course:** Data Warehouse and Business intelligence  
**Academic Semester:** 7th Semester  
**Implementation Platform:** Python 3.14.1, MySQL Community Server 8.0.46, Windows 11  
**GitHub Repository:** [retail-meshjoin-data-warehouse](https://github.com/Izhan-Nasir/retail-meshjoin-data-warehouse.git)  
**Submission Date:** November 13, 2026  

---

## Executive Summary

Modern enterprise retail organizations generate heterogeneous transactional data characterized by high-velocity continuous online sales streams interspersed with scheduled, large-scale periodic batch uploads from brick-and-mortar outlets. Bridging these operational streams into an integrated Analytical Data Warehouse requires high-throughput stream-table join processing without degrading database responsiveness.

This project designs, implements, and evaluates an end-to-end near-real-time and periodic Data Warehouse solution centered on the **MESHJOIN (Mesh Join)** algorithm. Using the UCI Online Retail dataset spanning 541,909 raw transactions, we established a strict five-step automated cleaning pipeline, generated deterministic multidimensional customer and product master data, implemented a multi-threaded execution architecture with priority buffering and adaptive load shedding, loaded a normalized star schema in MySQL, and authored 20 advanced OLAP analytical queries (Q1–Q20).

---

## 1. Data Warehouse Star Schema Architecture

### 1.1 Schema Design & Granularity
The warehouse follows a dimensional star schema optimized for fast analytical aggregation across customer demographics, geographic locations, product lines, suppliers, and temporal hierarchies. 

- **Fact Table Granularity:** One row per individual item/product purchased within a shopping cart (transaction). This atomic granularity preserves basket-level co-occurrence patterns (vital for market basket analysis in Q16) while enabling arbitrary rollups across dimensions.
- **Fact Table (`FACT_SALES`):**
  - Surrogate Foreign Keys: `customer_key`, `product_key`, `supplier_key`, `store_key`, `time_key`
  - Degenerate Dimension: `transaction_id` (Invoice number)
  - Numeric Measures: `quantity` (INT), `revenue` (DECIMAL(12,2))
  - Lineage Attributes: `sales_mode` (VARCHAR), `processing_mode` (VARCHAR)
- **Dimension Tables:**
  1. `DIM_CUSTOMER`: Surrogate `customer_key`, natural `customer_id`, `gender`, `age_group`, `occupation`, `city_category`, `stay_in_current_city_years`, `marital_status`.
  2. `DIM_PRODUCT`: Surrogate `product_key`, natural `product_id`, `product_name`, `product_category`.
  3. `DIM_SUPPLIER`: Surrogate `supplier_key`, natural `supplier_id`, `supplier_name`.
  4. `DIM_STORE`: Surrogate `store_key`, natural `store_id`, `store_name`, `city`.
  5. `DIM_TIME`: Surrogate `time_key`, natural unique composite `(event_date, event_hour)`, `day_of_month`, `month`, `quarter`, `year`, `day_of_week`, `is_weekend`, `season`, `half_year`.

```
                        +--------------------+
                        |    DIM_CUSTOMER    |
                        +--------------------+
                        | PK customer_key    |
                        |    customer_id     |
                        |    gender          |
                        |    age_group       |
                        |    occupation      |
                        |    city_category   |
                        +---------+----------+
                                  |
+-------------------+             |            +--------------------+
|    DIM_PRODUCT    |             |            |    DIM_SUPPLIER    |
+-------------------+             |            +--------------------+
| PK product_key    |             |            | PK supplier_key    |
|    product_id     |             |            |    supplier_id     |
|    product_name   |             |            |    supplier_name   |
|    product_cat    |             |            +---------+----------+
+---------+---------+             |                      |
          |           +-----------v------------+         |
          +---------->|       FACT_SALES       |<--------+
                      +------------------------+
                      | PK fact_key            |
                      | FK customer_key        |
                      | FK product_key         |
                      | FK supplier_key        |
                      | FK store_key           |
                      | FK time_key            |
                      |    transaction_id      |
                      |    quantity            |
                      |    revenue             |
                      |    sales_mode          |
                      |    processing_mode     |
                      +---+----------------+---+
                          |                |
+--------------------+    |                |   +--------------------+
|     DIM_STORE      |    |                |   |      DIM_TIME      |
+--------------------+    |                |   +--------------------+
| PK store_key       |<---+                +-->| PK time_key        |
|    store_id        |                         |    event_date      |
|    store_name      |                         |    event_hour      |
|    city            |                         |    day_of_month    |
+--------------------+                         |    month, quarter  |
                                               |    year, season    |
                                               |    is_weekend      |
                                               |    half_year       |
                                               +--------------------+
```

---

## 2. Data Cleaning Pipeline & Audit Metrics

Data cleansing was executed via an automated, reproducible pipeline (`cleaning.py`) adhering strictly to the university specification. The original dataset file (`Online Retail.xlsx`, 22.6 MB) was read in a read-only mode, and exact SHA-256 integrity checks confirmed that the raw source remained completely unaltered throughout the process.

### Cleaning Sequence & Row Deductions:
1. **Raw Records Ingested:** 541,909 rows.
2. **Step 1 — Cancellations Purged:** Transactions where `InvoiceNo` begins with `'C'` indicate cancelled or refunded orders. 
   - *Removed:* 9,288 rows. *Remaining:* 532,621 rows.
3. **Step 2 — Non-Positive Quantities Purged:** Records with $Quantity \le 0$ representing damaged stock, test entries, or system adjustments.
   - *Removed:* 1,336 rows. *Remaining:* 531,285 rows.
4. **Step 3 — Non-Positive Unit Prices Purged:** Records with $UnitPrice \le 0.0$ representing administrative write-offs or promotional giveaways.
   - *Removed:* 1,181 rows. *Remaining:* 530,104 rows.
5. **Step 4 — Missing Customer Identifiers Purged:** Anonymous checkout transactions where `CustomerID` was null/blank.
   - *Removed:* 132,220 rows. *Remaining:* 397,884 rows.
6. **Step 5 — Exact Duplicate Records Purged:** Identical duplicate transaction rows across all 8 original columns (per Human Decision #5).
   - *Removed:* 5,192 rows. *Final Cleaned Dataset:* **392,692 rows**.

### Special StockCode Handling:
Alphanumeric stock codes such as `POST` (Postage), `BANK CHARGES`, and suffix codes (e.g., `85123A`) were deliberately retained per Human Decision #6, ensuring no authentic merchandising transactions were discarded. `customer_id` was normalized to a clean integer string (eliminating trailing `.0` floating-point formatting).

---

## 3. Master Data Generation

Because the raw transactional dataset lacked external customer demographic and supplier tables, synthetic-yet-deterministic Master Data was generated using a portable hashing function `stable()`:
$$\text{stable}(key, n) = \text{int}(\text{MD5}(\text{key.encode('utf-8')})[:8], 16) \pmod n$$

### 3.1 Customer Master Data (`customer_master_data`)
- **Cardinality:** Exactly 4,338 unique customers.
- **Attributes:**
  - `gender`: `['Male', 'Female']` via `stable(customer_id, 2)`
  - `age_group`: `['0-17', '18-25', '26-35', '36-45', '46-50', '51-55', '55+']` via `stable(customer_id + 'age', 7)`
  - `occupation`: 10 professional categories via `stable(customer_id + 'occ', 10)`
  - `city_category`: `['A', 'B', 'C']` via `stable(customer_id + country, 3)` using the first chronological country for multi-national shoppers
  - `stay_in_current_city_years`: `['0', '1', '2', '3', '4+']` via `stable(customer_id + 'stay', 5)`
  - `marital_status`: `['Single', 'Married']` via `stable(customer_id + 'mar', 2)`
- **Partitions:** $\lceil 4,338 / 500 \rceil = 9$ disk partitions.

### 3.2 Product Master Data (`product_master_data`)
- **Cardinality:** Exactly 3,665 unique merchandise products.
- **Attributes:**
  - `product_name`: Longest recorded non-empty description for each code (ties broken by initial occurrence in file order; empty descriptions fallback to `'UNKNOWN'`).
  - `product_category`: 10 retail departments via `stable(product_id + 'cat', 10)`.
  - `supplier_id` & `supplier_name`: Deterministically selected from 5 regional suppliers (`SUP01` Northwind Supplies to `SUP05` EuroPack Partners) via `stable(product_id, 5)`.
- **Partitions:** $\lceil 3,665 / 500 \rceil = 8$ disk partitions.

---

## 4. Sales Mode, Store Allocation, and Periodic Staging

To simulate a real-world hybrid enterprise architecture, transactions were segmented into streaming and periodic batches at the cart (invoice) level:
$$\text{hash} = \text{int}(\text{MD5}(\text{str}(\text{int}(\text{invoice\_no})))[:8], 16) \pmod{10}$$
- $\text{hash} \in [0, 6]$ $\rightarrow$ **Online Stream** ($70.1\%$, 275,154 transactions written to `Data/online_sales_stream.csv`).
- $\text{hash} \in [7, 9]$ $\rightarrow$ **Periodic Batch** ($29.9\%$, 117,538 transactions staged in `periodic_sales_data`).

### Store Assignment:
- Online transactions are assigned strictly to `store_id = 1` (*Online / Web*).
- Periodic transactions are allocated to physical regional branches ($2$ to $5$) using:
  $$\text{store\_id} = 2 + (\text{int}(\text{MD5}(\text{str}(\text{int}(\text{transaction\_id})))[:8], 16) \pmod 4)$$
  *(2: London Bridge, 3: Manchester Arndale, 4: Birmingham Bullring, 5: Edinburgh Princes)*.
  Store enrichment is executed in memory outside MESHJOIN.

### Periodic Staging:
Periodic rows were sorted chronologically by `event_time` and partitioned into 48 balanced release groups ($117,538 / 48 \approx 2,449$ records per simulated hour). Each group carries a `release_hour` ($0$ to $47$) and an atomic `released` boolean flag to ensure groups are never doubly extracted.

---

## 5. MESHJOIN Algorithm Implementation

### 5.1 Architecture & Components
The MESHJOIN engine (`meshjoin.py`) joins high-velocity unbounded transactional streams against large, disk-resident relation partitions without requiring random disk seeks or full-table in-memory caching.

```
       Incoming Batch (up to 500 tx)
                   |
                   v
       +-----------------------+
       |   Queue of Chunks     | (FIFO, ages 0..9)
       +-----------+-----------+
                   |
        +----------+----------+
        |                     |
        v                     v
+---------------+     +---------------+
|  H_customer   |     |   H_product   | (Multi-Map Hash Tables)
+---------------+     +---------------+
        ^                     ^
        | Probed by           | Probed by
+---------------+     +---------------+
| Customer Disk |     | Product Disk  |
| Buffer (500)  |     | Buffer (500)  |
+---------------+     +---------------+
```

1. **StreamBuffer:** A thread-safe bounded buffer (capacity 1,000) using condition variables.
2. **Multi-Map Hash Tables ($H_C, H_P$):** Python `defaultdict(list)` structures storing pointers to transaction records. Multiple transactions sharing a customer or product ID coexist without overwriting.
3. **Queue:** A `collections.deque` holding chunk descriptors `(chunk_id, [transactions], age)`.
4. **Interleaved Dual-Pass Execution (Human Decision #2):**
   In each iteration:
   - Partition $c\_idx$ ($0 \le c\_idx < 9$) is read from `customer_master_data` (500 tuples via SQL `LIMIT 500 OFFSET offset`).
   - Partition $p\_idx$ ($0 \le p\_idx < 8$) is read from `product_master_data` (500 tuples via SQL `LIMIT 500 OFFSET offset`).
   - Up to 500 transactions are fetched from `StreamBuffer`, indexed into $H_C$ and $H_P$, and appended as a new chunk to the queue.
   - Customer and Product tuples probe $H_C$ and $H_P$, copying demographic and supplier attributes to matched records.
   - All chunks in the queue are aged by $+1$.
   - **Eviction Rule:** Any chunk reaching $\text{age} \ge \max(P_C, P_P) = \max(9, 8) = 9$ iterations has systematically traversed all 9 customer partitions and all 8 product partitions. It is popped from the queue, cleanly purged from $H_C$ and $H_P$, and emitted as 100% fully enriched.
   - Iteration indices advance: $c\_idx = (c\_idx + 1) \pmod 9$, $p\_idx = (p\_idx + 1) \pmod 8$.

---

## 6. Multi-Threading, Priority Handling, and Adaptive Load Shedding

### 6.1 Concurrent Worker Threads
- **OnlineThread:** Streams transactions from `online_sales_stream.csv` at a dynamically configurable rate ($80\text{ tx/s}$ in peak mode, $10\text{ tx/s}$ in non-peak mode). Stops at EOF without replay.
- **PeriodicThread:** Wakes once per simulated hour (60 seconds), extracts the next unreleased hourly group from `periodic_sales_data`, and updates `released = TRUE`.
- **MeshJoinThread:** Executes MESHJOIN join iterations continuously, routes fully enriched records to the consolidation layer, and coordinates shed recovery during non-peak operations.

### 6.2 Three-Level Priority Scheme
`StreamBuffer` maintains three internal priority deques drained in strict descending order:
1. **Priority 1 (Highest):** Online real-time stream transactions.
2. **Priority 2:** Scheduled periodic batch transactions.
3. **Priority 3 (Lowest):** Recovered transactions retrieved from the shed table.

### 6.3 Adaptive Load Shedding & Recovery Rules
- **Shedding Trigger:** If `StreamBuffer.size() >= 800` AND incoming transactions are periodic, load shedding activates.
  - *Partial Shedding:* If space exists for $k$ items ($k = 1,000 - \text{size}$), $k$ items are buffered and the remaining $N - k$ are diverted to `shed_table` with `flag = 'periodic'` and `shed_time = NOW()`.
  - *Online Inviolability:* Online transactions are **never shed under any circumstances**.
- **Recovery Trigger:** When the system operates in `non-peak` mode AND `StreamBuffer.size() < 500`, up to $500 - \text{size}$ records are read from `shed_table`, inserted into `StreamBuffer` at Priority 3, and deleted from `shed_table`.

---

## 7. Data Consolidation and Warehouse Loading

The consolidation module (`consolidation.py`) transforms enriched stream objects into permanent relational warehouse records:
1. **Temporal Derivation:** Extracts `event_date`, `event_hour`, `day_of_month`, `month`, `quarter`, `year`, `day_of_week`, `is_weekend` (Saturday/Sunday), `season` (Spring/Summer/Fall/Winter), and `half_year` (`H1`/`H2`).
2. **In-Memory Dimension Caching:** High-speed dictionaries (`customer_cache`, `product_cache`, `supplier_cache`, `store_cache`, `time_cache`) cache resolved surrogate integer keys, eliminating repeated `SELECT` queries against MySQL.
3. **Fact Table Loading:** Inserts records into `FACT_SALES` in bulk transactions with pre-computed $revenue = quantity \times unit\_price$ and enforces $processing\_mode = sales\_mode$.

---

## 8. Summary of OLAP Analytical Queries (Q1–Q20)

The SQL script `queries.sql` implements 20 sophisticated analytical queries executed directly against the live warehouse (outputs captured in `query_results.txt` across 137,000+ consolidated transactions):

### 8.1 Empirical Analytical Findings:
- **Q1 vs. Q11 (Product Performance & Temporal Drill-Down):** 
  - Q1 isolates the global top-5 revenue generators across the entire year: `REGENCY CAKESTAND 3 TIER` (£10,845.30 in Dec 2010 weekdays, £1,682.10 weekend), `MEDIUM CERAMIC TOP STORAGE JAR` (£77,183.60 in Jan 2011), `WHITE HANGING HEART T-LIGHT HOLDER` (£13,536.30 in Jan 2011), `PARTY BUNTING`, and `POSTAGE`.
  - Q11 dynamically recalculates the top-5 products for *each individual month*, capturing transient seasonal spikes that global aggregation masks.
- **Q2, Q3, Q4, Q10 (Demographic & Geographic Segmentation):**
  - **Q2:** Female customers in City Category C aged 26–35 represent the single highest-spending demographic group, generating **£179,323.68**, followed by Male customers aged 0–17 in City Category A (**£129,146.80**).
  - **Q3:** Professional segment analysis reveals Clerks generating the highest revenue in Garden supplies (£92,429.92), while Students dominated Home Decor and Jewelry.
  - **Q10:** Weekday sales consistently generate 4x to 8x the volume of weekend trading across all age demographics.
- **Q5 & Q8 (Categorical & Geographic Ranking via Window Functions):**
  - Uses `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` and `DENSE_RANK()` to rank product sales within each retail department and customer city category without self-joins.
- **Q7 & Q14 (Customer Loyalty & Seasonality):**
  - Customers with `0` recorded years of stay in their current city exhibit the highest average transaction values (£44.91 for females), representing high initial relocation basket sizes.
- **Q9, Q12, Q15 (Financial Velocity & Growth via `LAG()`):**
  - Monthly and quarterly revenue changes computed using `LAG(revenue, 1) OVER (...)`, properly returning `NULL` for baseline initialization periods without artificial zero distortions.
- **Q16 (Market Basket Analysis & Product Affinity):**
  - Implements a triangular self-join on `transaction_id` ($f_1.product\_key < f_2.product\_key$) with `COUNT(DISTINCT transaction_id)` to uncover top co-purchased shopping cart affinities:
    1. `GREEN REGENCY TEACUP AND SAUCER` & `ROSES REGENCY TEACUP AND SAUCER` (**194 joint purchases**)
    2. `WHITE HANGING HEART T-LIGHT HOLDER` & `RED HANGING HEART T-LIGHT HOLDER` (**156 joint purchases**)
    3. `SET OF 6 SPICE TINS PANTRY DESIGN` & `SET OF 3 CAKE TINS PANTRY DESIGN` (**152 joint purchases**)
    4. `HEART OF WICKER SMALL` & `HEART OF WICKER LARGE` (**149 joint purchases**)
    5. `GREEN REGENCY TEACUP AND SAUCER` & `PINK REGENCY TEACUP AND SAUCER` (**143 joint purchases**)
- **Q17 (Hierarchical Rollup Aggregations):**
  - Uses SQL `WITH ROLLUP` to generate simultaneous product totals, supplier subtotals, regional store subtotals, and the global enterprise grand total in a single scanning pass.
- **Q19 (Demand Spikes & Anomaly Detection):**
  - Isolates abnormal trading days where daily product revenue exceeded twice the product's historical daily average (over active trading days only). For example, `INFLATABLE POLITICAL GLOBE` spiked to £102.00 on 2011-04-01 (**5.44x spike ratio** over its £18.76 daily average).
- **Q20 (Quarterly Store Sales Materialization):**
  - Compiles the ordinary view `STORE_QUARTERLY_SALES`, detailing revenue and quantity trends across Birmingham Bullring, Edinburgh Princes, London Bridge, Manchester Arndale, and Online Web.

---

## 9. Critical Evaluation: Three Shortcomings of MESHJOIN

While MESHJOIN effectively eliminates the requirement to hold large tables in memory or perform high-latency random index lookups, empirical observation reveals three significant architectural limitations:

1. **Strict Sensitivity to Memory Budget vs. Arrival Rates:**
   MESHJOIN assumes that stream arrival velocity matches disk I/O throughput. If stream arrival rates surge significantly beyond disk partition load times, the Stream Buffer saturates immediately. Because chunks must remain in the hash table for a full cycle of master data partitions ($\max(P_C, P_P)$ iterations) to guarantee completeness, memory consumption is tightly bound to table partition counts.
2. **High Latency for Streaming Tuples:**
   Because a transaction is only emitted after traversing *all* disk partitions—even if its matching master data record was located in the very first partition—enrichment latency is uniformly pegged to the duration of 9 full iterations. It lacks an early-exit mechanism for completed joins without compromising hash table synchronization.
3. **Sequential Full-Scan Overhead & Skew Inefficiency:**
   MESHJOIN cyclically scans all master data partitions in sequential order regardless of data access frequency. In real-world retail, product sales exhibit extreme Zipfian skew (80% of sales derive from 20% of products). MESHJOIN expends identical I/O resources streaming rarely accessed "long-tail" product partitions as it does high-velocity popular items, without frequency-aware caching.

---

## 10. Reflections & Key Learnings

Building this system provided deep practical insights into:
- **Concurrent Stream Synchronization:** Managing multi-threaded producer-consumer queues using Python `threading.Condition` primitives to prevent race conditions and deadlocks under varying data arrival rates.
- **Stream-Table Join Algorithms:** Understanding the mechanical trade-offs between nested loop joins, symmetric hash joins, and memory-constrained partition scanning in MESHJOIN.
- **Database Indexing & Dimensional Modeling:** Witnessing firsthand how in-memory surrogate key caching and batch inserts reduce warehouse ingestion bottlenecks by over $90\%$.

---

## 11. Provenance & Citations

- **Dataset Title:** Online Retail Dataset
- **Repository:** UCI Machine Learning Repository
- **Reference:** Chen, D. (2015). Online Retail [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33
- **Dataset Retrieval Date:** **November 6, 2026** (per Human Decision #13)
- **Direct Fact Loading:** Direct batch fact loading was utilized throughout the pipeline; caching bonus was not implemented.

