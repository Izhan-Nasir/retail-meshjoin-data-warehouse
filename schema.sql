-- ============================================================
-- schema.sql — Star Schema DDL for the Retail Data Warehouse
-- Course: DS3003/DS3004 Data Warehousing & Business Intelligence
-- Drop order: view → fact → dimensions → working tables
-- This script is idempotent: safe to re-run from scratch.
-- ============================================================

-- ── 1. Drop view first (depends on no tables but must go before fact) ────────
DROP VIEW  IF EXISTS STORE_QUARTERLY_SALES;

-- ── 2. Drop fact table (has FKs to all dimensions) ──────────────────────────
DROP TABLE IF EXISTS FACT_SALES;

-- ── 3. Drop dimension tables ─────────────────────────────────────────────────
DROP TABLE IF EXISTS DIM_CUSTOMER;
DROP TABLE IF EXISTS DIM_PRODUCT;
DROP TABLE IF EXISTS DIM_SUPPLIER;
DROP TABLE IF EXISTS DIM_STORE;
DROP TABLE IF EXISTS DIM_TIME;

-- ── 4. Drop working / staging tables ─────────────────────────────────────────
DROP TABLE IF EXISTS periodic_sales_data;
DROP TABLE IF EXISTS shed_table;
DROP TABLE IF EXISTS customer_master_data;
DROP TABLE IF EXISTS product_master_data;

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION TABLES
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE DIM_CUSTOMER (
    customer_key            INT          NOT NULL AUTO_INCREMENT,
    customer_id             VARCHAR(20)  NOT NULL,          -- original ID
    gender                  VARCHAR(10)  NOT NULL,
    age_group               VARCHAR(10)  NOT NULL,
    occupation              VARCHAR(20)  NOT NULL,
    city_category           CHAR(1)      NOT NULL,
    stay_in_current_city_years VARCHAR(5) NOT NULL,
    marital_status          VARCHAR(10)  NOT NULL,
    PRIMARY KEY (customer_key),
    UNIQUE KEY uq_customer_id (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE DIM_SUPPLIER (
    supplier_key            INT          NOT NULL AUTO_INCREMENT,
    supplier_id             VARCHAR(10)  NOT NULL,          -- e.g. SUP01
    supplier_name           VARCHAR(60)  NOT NULL,
    PRIMARY KEY (supplier_key),
    UNIQUE KEY uq_supplier_id (supplier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE DIM_PRODUCT (
    product_key             INT          NOT NULL AUTO_INCREMENT,
    product_id              VARCHAR(20)  NOT NULL,          -- original StockCode
    product_name            TEXT         NOT NULL,
    product_category        VARCHAR(30)  NOT NULL,
    supplier_key            INT          NOT NULL,
    PRIMARY KEY (product_key),
    UNIQUE KEY uq_product_id (product_id),
    CONSTRAINT fk_product_supplier
        FOREIGN KEY (supplier_key) REFERENCES DIM_SUPPLIER (supplier_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE DIM_STORE (
    store_key               INT          NOT NULL AUTO_INCREMENT,
    store_id                INT          NOT NULL,          -- original 1–5
    store_name              VARCHAR(40)  NOT NULL,
    city                    VARCHAR(30)  NOT NULL,
    PRIMARY KEY (store_key),
    UNIQUE KEY uq_store_id (store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE DIM_TIME (
    time_key                INT          NOT NULL AUTO_INCREMENT,
    event_date              DATE         NOT NULL,
    event_hour              TINYINT      NOT NULL,          -- 0–23
    day_of_month            TINYINT      NOT NULL,          -- 1–31
    month                   TINYINT      NOT NULL,          -- 1–12
    quarter                 TINYINT      NOT NULL,          -- 1–4
    year                    SMALLINT     NOT NULL,          -- e.g. 2010/2011
    day_of_week             VARCHAR(10)  NOT NULL,          -- Monday … Sunday
    is_weekend              TINYINT(1)   NOT NULL,          -- 0=weekday 1=weekend
    season                  VARCHAR(10)  NOT NULL,          -- Spring/Summer/Fall/Winter
    half_year               CHAR(2)      NOT NULL,          -- H1 or H2
    PRIMARY KEY (time_key),
    UNIQUE KEY uq_date_hour (event_date, event_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT TABLE
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE FACT_SALES (
    fact_key                INT              NOT NULL AUTO_INCREMENT,
    transaction_id          VARCHAR(20)      NOT NULL,      -- original InvoiceNo
    customer_key            INT              NOT NULL,
    product_key             INT              NOT NULL,
    supplier_key            INT              NOT NULL,
    store_key               INT              NOT NULL,
    time_key                INT              NOT NULL,
    quantity                INT              NOT NULL,
    revenue                 DECIMAL(12, 2)   NOT NULL,      -- quantity × unit_price
    sales_mode              VARCHAR(10)      NOT NULL,      -- 'online' | 'periodic'
    processing_mode         VARCHAR(10)      NOT NULL,      -- same as sales_mode (AMB-01)
    PRIMARY KEY (fact_key),
    CONSTRAINT fk_fact_customer  FOREIGN KEY (customer_key)  REFERENCES DIM_CUSTOMER (customer_key),
    CONSTRAINT fk_fact_product   FOREIGN KEY (product_key)   REFERENCES DIM_PRODUCT  (product_key),
    CONSTRAINT fk_fact_supplier  FOREIGN KEY (supplier_key)  REFERENCES DIM_SUPPLIER (supplier_key),
    CONSTRAINT fk_fact_store     FOREIGN KEY (store_key)     REFERENCES DIM_STORE    (store_key),
    CONSTRAINT fk_fact_time      FOREIGN KEY (time_key)      REFERENCES DIM_TIME     (time_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Index to speed up Q16 (product-pair self-join on transaction_id)
CREATE INDEX idx_fact_transaction ON FACT_SALES (transaction_id);
-- Index for time-based filtering
CREATE INDEX idx_fact_time ON FACT_SALES (time_key);

-- ─────────────────────────────────────────────────────────────────────────────
-- WORKING / STAGING TABLES (not part of the star schema)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE periodic_sales_data (
    id              INT          NOT NULL AUTO_INCREMENT,
    transaction_id  VARCHAR(20)  NOT NULL,
    customer_id     VARCHAR(20)  NOT NULL,
    product_id      VARCHAR(20)  NOT NULL,
    quantity        INT          NOT NULL,
    unit_price      DECIMAL(10, 2) NOT NULL,
    event_time      DATETIME     NOT NULL,
    sales_mode      VARCHAR(10)  NOT NULL DEFAULT 'periodic',
    release_hour    TINYINT      NOT NULL,                  -- 0–47
    released        TINYINT(1)   NOT NULL DEFAULT 0,        -- 0=not yet, 1=done
    PRIMARY KEY (id),
    INDEX idx_psd_release (release_hour, released)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE shed_table (
    id              INT          NOT NULL AUTO_INCREMENT,
    transaction_id  VARCHAR(20)  NOT NULL,
    customer_id     VARCHAR(20)  NOT NULL,
    product_id      VARCHAR(20)  NOT NULL,
    quantity        INT          NOT NULL,
    unit_price      DECIMAL(10, 2) NOT NULL,
    event_time      DATETIME     NOT NULL,
    sales_mode      VARCHAR(10)  NOT NULL,
    flag            VARCHAR(20)  NOT NULL DEFAULT 'shed',
    shed_time       DATETIME     NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE customer_master_data (
    customer_id                VARCHAR(20)  NOT NULL,
    gender                     VARCHAR(10)  NOT NULL,
    age_group                  VARCHAR(10)  NOT NULL,
    occupation                 VARCHAR(20)  NOT NULL,
    city_category              CHAR(1)      NOT NULL,
    stay_in_current_city_years VARCHAR(5)   NOT NULL,
    marital_status             VARCHAR(10)  NOT NULL,
    PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE product_master_data (
    product_id       VARCHAR(20)  NOT NULL,
    product_name     TEXT         NOT NULL,
    product_category VARCHAR(30)  NOT NULL,
    supplier_id      VARCHAR(10)  NOT NULL,
    supplier_name    VARCHAR(60)  NOT NULL,
    PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW (Q20: SELECT uses ORDER BY at query time, not inside CREATE VIEW)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE VIEW STORE_QUARTERLY_SALES AS
    SELECT
        s.store_name,
        t.year,
        t.quarter,
        SUM(f.revenue) AS total_revenue
    FROM  FACT_SALES   f
    JOIN  DIM_STORE    s ON f.store_key = s.store_key
    JOIN  DIM_TIME     t ON f.time_key  = t.time_key
    GROUP BY s.store_name, t.year, t.quarter;
-- NOTE: ORDER BY is intentionally omitted here (MySQL ignores ORDER BY in views).
-- Use: SELECT * FROM STORE_QUARTERLY_SALES ORDER BY store_name, year, quarter;
