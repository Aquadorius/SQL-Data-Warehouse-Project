/*
========================================================
SILVER LAYER ETL PROCEDURE: Complete Data Transformation Pipeline
========================================================

Stored Procedure: silver.load_silver
Database: DataWarehouse
Schema: silver
File: scripts/silver/load_silver_procedure.sql

Purpose
-------
This stored procedure orchestrates the complete ETL pipeline from bronze 
to silver layer, transforming raw source data into cleaned, standardized, 
and business-ready format. It processes data from both CRM and ERP systems 
with comprehensive data quality improvements and business rule applications.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME2, @end_time DATETIME2, @batch_start_time DATETIME2, @batch_end_time DATETIME2
	SET @batch_start_time=GETDATE();
	BEGIN TRY
		SET @start_time=GETDATE();
		PRINT('===============================');
		PRINT('LOADING THE SILVER LAYER');
		PRINT('===============================');
		
		PRINT('-------------------------------');
		PRINT('Loading CRM Tables;');
		PRINT('-------------------------------');
		PRINT('TRUNCATING TABLE: silver.crm_cust_info');
		IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
			TRUNCATE TABLE silver.crm_cust_info;
		PRINT('INSERTING DATA INTO TABLE: silver.crm_cust_info');
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

			select 
			c.cst_id,
			c.cst_key,
			TRIM(c.cst_firstname) cst_firstname,
			TRIM(c.cst_lastname) cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
				ELSE 'n/a'
			END as cst_martial_status,

			CASE 
				WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
				ELSE 'n/a'
			END cst_gndr,
			c.cst_create_date
			from(
				--No Duplicates or Nulls in Primary key
				select* from
					(
					select *,
					ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) flag_last
					from bronze.crm_cust_info
					where cst_id is not null)t
				where flag_last=1
				)c;
			SET @end_time=GETDATE();
			PRINT('Load Duration:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds');
		
		
		PRINT('===================================');
		PRINT('INSERTING TRANSFORMED CLEANED DATA INTO silver.crm_prd_info');
		SET @start_time=GETDATE();
		IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
		BEGIN	
			DROP TABLE silver.crm_prd_info;
		END;
		

		CREATE TABLE silver.crm_prd_info (
			prd_id INT,                                    -- PRIMARY KEY: Product identifier
			cat_id NVARCHAR(50),                          -- DERIVED COLUMN: Category ID for ERP joins (from prd_key)
			sls_prd_key NVARCHAR(50),                     -- DERIVED COLUMN: Sales key for transaction joins (from prd_key)
			prd_nm NVARCHAR(50),                          -- PRODUCT NAME: Product description
			prd_cost INT,                                 -- PRODUCT COST: Unit cost (NULL values converted to 0)
			prd_line NVARCHAR(50),                        -- PRODUCT LINE: Standardized category (M→Mountain, R→Road, etc.)
			prd_start_dt DATE,                            -- EFFECTIVE START: When product version became active
			prd_end_dt DATE,                              -- EFFECTIVE END: When product version ended (calculated)
			dwh_create_date DATETIME2 DEFAULT GETDATE()   -- AUDIT COLUMN: ETL load timestamp
		);
		PRINT('TRUNCATING TABLE: silver.crm_prd_info');
		IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
			TRUNCATE TABLE silver.crm_prd_info;
		PRINT('INSERTING INTO TABLE: silver.crm_prd_info');
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		sls_prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		select
		prd_id,
		Replace(SUBSTRING(TRIM(prd_key),1,5),'-','_') cat_id,--Purpose of column: to be able to join with erp_px_cat_g1v2
		SUBSTRING(TRIM(prd_key),7,len(prd_key)) as sales_prd_key,--To be able to join with crm_sales_details table primary key sls_prd_key
		prd_nm,
		COALESCE(prd_cost,0) AS  prd_cost,
		CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		ELSE 'n/a'
		END prd_line,
		prd_start_dt,
		lead(dateadd(day,-1,prd_start_dt)) over(partition by prd_key order by prd_start_dt) as prd_end_dt
		from bronze.crm_prd_info 
		order by prd_id;
		SET @end_time=GETDATE();
		PRINT('Load Duration:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds');
		PRINT('=====================================');

		

		/*===================================
		LOADING CLEANED DATA INTO silver.crm_sales_details
		=====================================*/

		PRINT('====================================================');
		PRINT('---------------------------------------------------');
		PRINT('LOADING CLEANED DATA INTO silver.crm_sales_details');
		PRINT('---------------------------------------------------');
		SET @start_time=GETDATE();
		PRINT('DROPPING TABLE: silver.crm_sales_details');
		IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
			DROP TABLE silver.crm_sales_details;
		PRINT('CREATING TABLE: silver.crm_sales_details');
		CREATE TABLE silver.crm_sales_details(
			sls_ord_num VARCHAR(50),
			sls_prd_key VARCHAR(50),
			sls_cust_id INT,
			sls_order_dt DATE,
			sls_ship_dt DATE,
			sls_due_dt DATE,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT,
			dwh_create_date DATETIME2 DEFAULT GETDATE(),
			dwh_update_date DATETIME2 NULL
		);
		PRINT('TRUNCATING TABLE: silver.crm_sales_details');
		IF OBJECT_ID('silver.crm_sales_details','U')IS NOT NULL
		BEGIN TRUNCATE TABLE silver.crm_sales_details
		END;
		PRINT('INSERTING DATA INTO TABLE: silver.crm_sales_details');
		
		INSERT INTO  silver.crm_sales_details(	
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price)

			SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN LEN(sls_order_dt)<>8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
			END sls_order_dt,
			CASE WHEN LEN(sls_ship_dt)<>8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
			END sls_ship_dt,
			CASE WHEN LEN(sls_due_dt)<>8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales<=0 or sls_sales is null or sls_sales<>sls_quantity*ABS(sls_price)
			THEN ABS(sls_price)*sls_quantity
			ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price<=0 or sls_price is null THEN  sls_sales/nullif(sls_quantity,0)
			ELSE sls_price
			END sls_price
			FROM bronze.crm_sales_details;
			SET @end_time=GETDATE();
			PRINT('Load Duration:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds');
			PRINT('================================================');
		

		/*===================================
		INSERTING TRANSFORMED CLEANED DATA INTO silver.erp_cust_az12
		=====================================*/
		PRINT('-------------------------------');
		PRINT('Loading CRM Tables');
		PRINT('-------------------------------');
		PRINT('======================================');
		PRINT('LOADING CLEANED DATA INTO silver.erp_cust_az12');
		
		SET @start_time=GETDATE()
		PRINT('>>TRUNCATING TABLE: silver.erp_cust_az12');
		IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
		BEGIN TRUNCATE TABLE silver.erp_cust_az12;
		END;
		PRINT('>>INSERTING DATA INTO TABLE: silver.erp_cust_az12');
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		select 
		CASE WHEN cid like 'NAS%' THEN substring(cid,4,len(cid))--Removes NAS prefix if present
			ELSE cid
		END cid,
		CASE WHEN bdate>GETDATE() THEN NULL--sets future birthdates null 
			ELSE bdate
		END bdate,
		CASE WHEN UPPER(TRIM(gen)) ='F' THEN 'Female'
			WHEN UPPER(TRIM(gen))='M'THEN 'MALE'
			WHEN gen IS NULL OR gen='' THEN 'n/a'
			ELSE gen
		END gen--Nomalize gender values and handle unknown cases
		from bronze.erp_cust_az12;
		SET @end_time=GETDATE()
			PRINT('Load Duration:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds');
			PRINT('================================================');

		/*===================================
		INSERTING TRANSFORMED CLEANED DATA INTO silver.erp_loc_a101
		=====================================*/
		PRINT('=======================================');
		PRINT('---------------------------------------');
		PRINT('LOADING TRANSFORMED DATA INTO: silver.erp_loc_a101 ')
		PRINT('---------------------------------------')
		PRINT('TRUNCATING TABLE: silver.erp_loc_a101');
		
		SET @start_time=GETDATE();
		IF OBJECT_ID('silver.erp_loc_a101') IS NOT NULL 
		BEGIN TRUNCATE TABLE silver.erp_loc_a101
		END;
		PRINT('INSERTING DATA INTO TABLE: silver.erp_loc_a101');
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT
			REPLACE(cid,'-','')cid ,
			CASE WHEN UPPER(TRIM(cntry)) IN ('USA','US','UNITED STATES') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) IN('DE','GERMANY') THEN 'Germany'
				WHEN cntry IS NULL OR TRIM(cntry)='' THEN 'n/a'
				ELSE TRIM(cntry)
			END cntry
		FROM bronze.erp_loc_a101;
		SET @end_time=GETDATE();
			PRINT('Load Duration:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds');
			PRINT('================================================');

		/*===================================
		INSERTING TRANSFORMED CLEANED DATA INTO silver.erp_px_cat_g1v2
		--There was nothing to clean
		=====================================*/

		PRINT('=======================================');
		PRINT('---------------------------------------');
		PRINT('LOADING TRANSFORMED DATA INTO: silver.erp_px_cat_g1v2 ')
		PRINT('---------------------------------------')
		SET @start_time=GETDATE();
		PRINT('TRUNCATING TABLE: silver.erp_px_cat_g1v2');
		IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
		BEGIN TRUNCATE TABLE silver.erp_px_cat_g1v2
		END;
		PRINT('INSERTING DATA INTO TABLE: silver.erp_px_cat_g1v2');
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @start_time=GETDATE();
		PRINT('Load Duration:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds');
		PRINT('================================================');

END TRY
BEGIN CATCH
		PRINT('===========================================');
		PRINT('ERROR OCCURRED DURING LOADING BRONZE LAYER');
		PRINT('ERROR MESSAGE: '+ ERROR_MESSAGE());
		PRINT('ERROR NUMBER: '+CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('ERROR STATE: '+CAST(ERROR_STATE() AS NVARCHAR));
END CATCH
SET @batch_end_time=GETDATE();

PRINT('=======================================')
PRINT('TOTAL LOADING TIME FOR SILVER LAYER: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds')
PRINT('=======================================')
END;

EXEC silver.load_silver
