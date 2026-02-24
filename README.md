# 🏗️ SQL Data Warehouse Project

A fully structured **Medallion Architecture** data warehouse built with **SQL Server**, ingesting raw CRM and ERP data through Bronze → Silver → Gold transformation layers into a clean star schema ready for analytics and BI reporting.

---

## 📁 Repository Structure

```
SQL-Data-Warehouse-Project/
├── datasets/
│   ├── source_crm/          # Raw CRM CSV files (cust_info, prd_info, sales_details)
│   └── source_erp/          # Raw ERP CSV files (CUST_AZ12, LOC_A101, PX_CAT_G1V2)
├── scripts/
│   ├── bronze/
│   │   ├── bronze_ddl.sql             # Bronze table definitions
│   │   └── bronze_load_procedure.sql  # Stored procedure: CSV → Bronze
│   ├── silver/
│   │   ├── silver_ddl.sql             # Silver table definitions
│   │   └── silver_load_procedure.sql  # Stored procedure: Bronze → Silver
│   └── gold/
│       └── gold_views.sql             # Gold layer views (Star Schema)
├── tests/
│   └── gold_quality_checks.sql        # Data quality validation queries
├── docs/                              # Architecture diagrams and documentation
├── LICENSE
└── README.md
```

---

## 🏛️ Architecture Overview: Medallion Layers

This project implements the **Medallion (Bronze / Silver / Gold)** architecture pattern — a staged approach to data transformation that ensures quality, traceability, and reusability at every layer.

```
 ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
 │   SOURCE     │ ───▶ │    BRONZE    │ ───▶ │    SILVER    │ ───▶ 🏅 GOLD
 │  CSV Files   │      │  Raw Ingest  │      │  Cleaned &   │      Star Schema
 │  CRM + ERP   │      │  No changes  │      │ Standardised │      Analytics-Ready
 └──────────────┘      └──────────────┘      └──────────────┘
```

---

## 🥉 Bronze Layer — Raw Ingestion

**Purpose:** Store raw source data as-is, preserving full fidelity of the originals.

**How data arrives:** Via `BULK INSERT` from CSV files into SQL Server tables.

**Tables Created:**

| Table | Source System | Description |
|---|---|---|
| `bronze.crm_cust_info` | CRM | Customer master data |
| `bronze.crm_prd_info` | CRM | Product master data |
| `bronze.crm_sales_details` | CRM | Sales transactions |
| `bronze.erp_cust_az12` | ERP | Additional customer attributes |
| `bronze.erp_loc_a101` | ERP | Customer location data |
| `bronze.erp_px_cat_g1v2` | ERP | Product category mapping |

**Key Design Decisions:**
- Date fields (`sls_order_dt`, `sls_ship_dt`, `sls_due_dt`) are stored as `INT` to match the raw source format — conversion happens in Silver.
- No transformations are applied — the Bronze layer is a faithful snapshot of the source.
- Tables are **truncated and reloaded** on every run (full refresh pattern).

**To load the Bronze layer:**
```sql
EXEC bronze.load_bronze;
```

---

## 🥈 Silver Layer — Cleaned & Standardised

**Purpose:** Apply data quality rules, standardise values, remove duplicates, and prepare data for joins across systems.

**Tables Created:**

| Table | Source | Key Transformations |
|---|---|---|
| `silver.crm_cust_info` | `bronze.crm_cust_info` | Deduplication, TRIM whitespace, decode gender & marital status codes |
| `silver.crm_prd_info` | `bronze.crm_prd_info` | Derive `cat_id` and `sls_prd_key` from `prd_key`, decode product line codes, calculate `prd_end_dt` via `LEAD()`, replace NULL costs with 0 |
| `silver.crm_sales_details` | `bronze.crm_sales_details` | Convert integer dates to `DATE`, validate and recalculate `sls_sales` and `sls_price` |
| `silver.erp_cust_az12` | `bronze.erp_cust_az12` | Strip `NAS` prefix from customer IDs, nullify future birthdates, normalise gender values |
| `silver.erp_loc_a101` | `bronze.erp_loc_a101` | Remove hyphens from IDs, standardise country names (USA/US → `United States`, DE/Germany → `Germany`) |
| `silver.erp_px_cat_g1v2` | `bronze.erp_px_cat_g1v2` | Loaded as-is (no cleaning required) |

**All Silver tables include audit columns:**
```sql
dwh_create_date  DATETIME2  DEFAULT GETDATE()   -- When the record was loaded
dwh_update_date  DATETIME2  NULL                -- For future SCD tracking
```

### Transformation Details

**Customer Deduplication (`crm_cust_info`)**
```sql
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)
-- Keeps only the most recent record per customer
```

**Code Decoding (Gender, Marital Status, Product Line)**
```
M → Male / Married / Mountain
F → Female
S → Single
R → Road
T → Touring
S → Other Sales
```

**Integer Date Conversion (`crm_sales_details`)**
```sql
CASE WHEN LEN(sls_order_dt) <> 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END
```

**Product Key Parsing (`crm_prd_info`)**
```sql
-- prd_key format: "XX-XX-ProductCode"
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id      -- e.g. "AC_HE" → joins to erp_px_cat_g1v2
SUBSTRING(prd_key, 7, LEN(prd_key))         AS sls_prd_key  -- e.g. "HL-U509" → joins to crm_sales_details
```

**Sales Validation (`crm_sales_details`)**
```sql
-- Recalculate sales if null, negative, or inconsistent with qty × price
CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales <> sls_quantity * ABS(sls_price)
     THEN ABS(sls_price) * sls_quantity
     ELSE sls_sales
END
```

**To load the Silver layer:**
```sql
EXEC silver.load_silver;
```

---

## 🥇 Gold Layer — Star Schema (Analytics-Ready)

**Purpose:** Expose clean, business-friendly views modelled as a **star schema** for use in BI tools, dashboards, and ad-hoc analysis.

**Components:**

### Dimension Views

**`gold.dim_customers`** — Unified customer profile (CRM + ERP joined)
```
silver.crm_cust_info
    LEFT JOIN silver.erp_cust_az12   (birth date, gender enrichment)
    LEFT JOIN silver.erp_loc_a101    (country)
```

| Column | Description |
|---|---|
| `customer_key` | Surrogate key (generated via `ROW_NUMBER()`) |
| `customer_id` | Source CRM customer ID |
| `customer_number` | CRM customer key |
| `first_name`, `last_name` | Name fields |
| `country` | From ERP location table |
| `marital_status` | Decoded from CRM |
| `gender` | CRM value; ERP value used as fallback if CRM is `n/a` |
| `birth_date` | From ERP customer attributes |
| `create_date` | Original CRM registration date |

**`gold.dim_products`** — Product catalogue with category info (currently active products only)
```
silver.crm_prd_info
    LEFT JOIN silver.erp_px_cat_g1v2   (category, subcategory, maintenance)
WHERE prd_end_dt IS NULL  -- Active products only
```

| Column | Description |
|---|---|
| `product_key` | Surrogate key |
| `product_id` | Source product ID |
| `product_number` | Sales key used in transactions |
| `product_name` | Full product name |
| `category_id`, `category`, `subcategory` | From ERP category table |
| `maintenance` | Maintenance flag from ERP |
| `cost` | Unit cost |
| `product_line` | Decoded product line (Mountain, Road, Touring, etc.) |
| `start_date`, `end_date` | Product version effective dates |

### Fact View

**`gold.fact_sales`** — Sales transactions linked to dimensions
```
silver.crm_sales_details
    LEFT JOIN gold.dim_customers   (on customer_id)
    LEFT JOIN gold.dim_products    (on product_number)
```

| Column | Description |
|---|---|
| `order_number` | Sales order identifier |
| `customer_key` | FK → `dim_customers` |
| `product_key` | FK → `dim_products` |
| `order_date`, `ship_date`, `due_date` | Transaction dates |
| `sales` | Total sales amount |
| `quantity` | Units sold |
| `price` | Unit price |

### Star Schema Diagram

```
                    ┌─────────────────────┐
                    │   gold.dim_products │
                    │─────────────────────│
                    │ product_key (PK)    │
                    │ product_id          │
                    │ product_number      │
                    │ product_name        │
                    │ category            │
                    │ subcategory         │
                    │ cost                │
                    │ product_line        │
                    └──────────┬──────────┘
                               │
┌──────────────────────┐       │       ┌──────────────────────┐
│  gold.dim_customers  │       │       │   gold.fact_sales    │
│──────────────────────│       │       │──────────────────────│
│ customer_key (PK)    ├───────┼───────┤ order_number         │
│ customer_id          │       └───────┤ customer_key (FK)    │
│ customer_number      │               │ product_key (FK)     │
│ first_name           │               │ order_date           │
│ last_name            │               │ ship_date            │
│ country              │               │ due_date             │
│ marital_status       │               │ sales                │
│ gender               │               │ quantity             │
│ birth_date           │               │ price                │
└──────────────────────┘               └──────────────────────┘
```

---

## 🚀 Getting Started

### Prerequisites
- SQL Server 2019+ (or Azure SQL)
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Access to the CSV source files

### Setup Steps

**1. Create the database and schemas**
```sql
-- Run: scripts/init_database.sql
-- Creates DataWarehouse database + bronze, silver, gold schemas
```

**2. Create Bronze tables**
```sql
-- Run: scripts/bronze/bronze_ddl.sql
```

**3. Load Bronze layer**
```sql
-- Update file paths in bronze_load_procedure.sql to match your environment
-- Run: scripts/bronze/bronze_load_procedure.sql
EXEC bronze.load_bronze;
```

**4. Create Silver tables**
```sql
-- Run: scripts/silver/silver_ddl.sql
```

**5. Load Silver layer**
```sql
-- Run: scripts/silver/silver_load_procedure.sql
EXEC silver.load_silver;
```

**6. Create Gold layer views**
```sql
-- Run: scripts/gold/gold_views.sql
```

**7. Validate the data**
```sql
-- Run: tests/gold_quality_checks.sql
SELECT * FROM gold.fact_sales;
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
```

---

## 🔄 ETL Pipeline Flow

```
CSV Files (CRM + ERP)
        │
        ▼  BULK INSERT
  ┌─────────────┐
  │   BRONZE    │  Raw tables — full refresh on every run
  └──────┬──────┘
         │
         ▼  Stored Procedure: silver.load_silver
  ┌─────────────┐
  │   SILVER    │  Cleaned, deduplicated, standardised
  └──────┬──────┘
         │
         ▼  CREATE VIEW
  ┌─────────────┐
  │    GOLD     │  Star schema — dim_customers, dim_products, fact_sales
  └─────────────┘
         │
         ▼
  BI Tools / Reporting / Ad-hoc Analysis
```

---

## 🧪 Data Quality Checks

Quality validation scripts are located in `/tests/gold_quality_checks.sql` and cover:
- Referential integrity between fact and dimension tables
- Null checks on key columns
- Duplicate surrogate key detection
- Sales amount validation (sales = quantity × price)
- Date range consistency checks

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Database | Microsoft SQL Server |
| ETL | T-SQL Stored Procedures |
| Data Loading | `BULK INSERT` from CSV |
| Data Modelling | Medallion Architecture (Bronze/Silver/Gold) |
| Analytics Layer | Star Schema (Views) |
| Version Control | Git / GitHub |

---

## ⚠️ Notes & Warnings

- **File Paths:** The Bronze load procedure uses hard-coded local file paths. Update these to match your environment before running.
- **Data Overwrite:** All load procedures use `TRUNCATE` before inserting — existing data will be replaced on every run.
- **Gold Layer:** Implemented as SQL **Views** (not materialised tables), so they always reflect the latest Silver data automatically.
- **Active Products Only:** `gold.dim_products` filters to `prd_end_dt IS NULL`, showing only currently active product versions.

---

## 📄 License

This project is licensed under the terms in the [LICENSE](./LICENSE) file.
