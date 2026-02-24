/*
========================================================
Data Warehouse Database Creation
========================================================

Overview
--------
This section ensures that a fresh database named 
`DataWarehouse` is created for our ETL/ELT pipeline.  
It is the central repository where all schemas (bronze, 
silver, gold) are maintained.  

What the Code Does
------------------
1. Switches context to the `master` database.  
2. Drops the `DataWarehouse` database if it already exists, 
   ensuring a clean environment for re-runs.  
3. Creates a new `DataWarehouse` database.  
4. Switches context to `DataWarehouse`.  
5. Creates the core schemas:  
   - **bronze** → Raw source data (CRM, ERP, etc.)  
   - **silver** → Cleaned, transformed, standardized data  
   - **gold**   → Business-ready curated data for reporting  
========================================================
*/
-- Check if database exists and drop it
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    PRINT ('Database DataWarehouse found. Proceeding to drop...');
    
    -- Step 1: Set database to single user mode to disconnect all users
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    PRINT ('Database set to single user mode.');
    
    -- Step 2: Drop the database
    DROP DATABASE DataWarehouse;
    PRINT ('Database DataWarehouse dropped successfully.');
END
ELSE
BEGIN
    PRINT ('Database DataWarehouse does not exist.');
END
GO
-- Step 3: Create the database
CREATE DATABASE DataWarehouse;
GO
PRINT ('Database DataWarehouse created successfully.');

-- Step 4: Switch to the new database
USE DataWarehouse;
GO
-- Step 5: Create schemas
CREATE SCHEMA bronze;
GO
PRINT ('Schema bronze created successfully.');

GO
CREATE SCHEMA silver;
GO
PRINT ('Schema silver created successfully.');
GO
CREATE SCHEMA gold;
GO
PRINT ('Schema gold created successfully.');
GO
PRINT( '========================================');
PRINT ('Database and schemas setup completed successfully!');
PRINT ('Ready for table creation and data loading.');
PRINT ('========================================');



/*
========================================================
Bronze Layer Table Creation Script
========================================================

Overview
--------
This script creates the bronze layer tables for storing 
raw data ingested from CRM and ERP source systems.  
The bronze layer is intended to store unaltered source 
data as close to the original as possible, preserving 
the integrity of the raw information.  

Source Systems Covered:
- CRM System  
  - crm_cust_info – Customer master data  
  - crm_sales_details – Sales transactions  
  - crm_prd_info – Product master data  
- ERP System  
  - erp_cust_az_12 – Additional customer attributes  
  - erp_loc_a101 – Customer location data  
  - erp_px_cat_g1v2 – Product category mapping  

Warnings
--------
- Drop Behavior:
  - Each table is dropped if it already exists (IF OBJECT_ID ... DROP TABLE). 
    This ensures fresh creation but will remove existing data.
- Raw Dates in Source:
  - In the bronze layer, some dates (sls_order_dt, sls_ship_dt, sls_due_dt) 
    are kept as integers because the source system provided them as numeric 
    values. These should be cleaned/converted in the silver layer.  

What the Code Does
------------------
1. Drops the table if it already exists.
2. Recreates the table with the schema and datatypes defined.
3. Segregates CRM and ERP data structures for clarity.
4. Prepares the foundation for transformations in the silver/gold layers.
========================================================
*/

-- Creating Table for the source data crm and erp
-- Use NVARCHAR if you have data in languages other than English

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(15),
	cst_firstname VARCHAR(20),
	cst_lastname VARCHAR(20),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR (10),
	cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

IF OBJECT_ID('bronze.crm_prd_info','U') is not null
DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE);

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(10)
);

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id          NVARCHAR(50),
	cat         NVARCHAR(50),
	subcat      NVARCHAR(50),
	maintenance NVARCHAR(50)
);



