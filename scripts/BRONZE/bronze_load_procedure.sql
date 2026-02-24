/*
====================================================================
Stored Procedure: Load Bronze Layer (Source CSVs -> Bronze Tables)
====================================================================

Script Purpose:
---------------
This stored procedure automates the loading of raw data 
from external CSV files into the **bronze schema**.  
It ensures a repeatable, consistent, and traceable ETL 
process by performing the following actions:

1. Logs the start and end time of the entire batch load.  
2. Prints progress messages for better monitoring.  
3. Truncates each target table before inserting data to 
   avoid duplication.  
4. Loads data into CRM and ERP bronze tables using 
   the `BULK INSERT` command.  
5. Captures and prints load duration for each table.  
6. Provides structured error handling with error messages, 
   number, and state if the process fails.  

Tables Loaded:
--------------
- CRM System Tables:  
  • bronze.crm_cust_info  
  • bronze.crm_prd_info  
  • bronze.crm_sales_details  

- ERP System Tables:  
  • bronze.erp_cust_az12  
  • bronze.erp_loc_a101  
  • bronze.erp_px_cat_g1v2  

Warnings:
---------
- **Data Overwrite:**  
  The procedure truncates all bronze tables before loading.  
  Any existing data will be lost.  
- **File Paths:**  
  File locations are hard-coded (C:\Users\...). Ensure the 
  CSVs exist at those paths or adjust accordingly.  
- **Access Rights:**  
  SQL Server must have sufficient permissions to read from 
  the file system location.  

Parameters:
-----------
None.  
This stored procedure does not accept parameters or return values.  

Usage Example:
--------------
EXEC bronze.load_bronze;  

====================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
	BEGIN TRY 
		SET @batch_start_time=GETDATE();
		PRINT('===============================');
		PRINT('LOADING THE BRONZE LAYER');
		PRINT('===============================');

		PRINT('-------------------------------');
		PRINT('Loading CRM Tables');
		PRINT('-------------------------------');

		SET @start_time= GETDATE();
		PRINT('>>Truncating Table:  bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT('>>Inserting Data into: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Elite 1040 G5\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK);
		SET @end_time=GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds');
		

		SET @start_time=GETDATE();
		PRINT('>>Truncating Table: bronze.crm_prd_info');
		TRUNCATE TABLE  bronze.crm_prd_info;
		PRINT('>>Inserting Data into: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Elite 1040 G5\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK);
		SET @end_time=GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds');



		SET @start_time=GETDATE();
		PRINT('>>Truncating Table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT('>>Inserting Data into: bronze.crm_sales_detail');
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Elite 1040 G5\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK);
		SET @end_time=GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds');


		PRINT('-------------------------------');
		PRINT('Loading CRM Tables;');
		PRINT('-------------------------------');

		SET @start_time=GETDATE();
		PRINT('>> Truncating Table: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT('>> Inserting Data into: bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Elite 1040 G5\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK);
		SET @end_time=GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds');

		SET @start_time=GETDATE();
		PRINT('>> Truncating Table: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT('>> Inserting Data into: bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Elite 1040 G5\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK);
		SET @end_time=GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds');

		SET @start_time=GETDATE();
		PRINT('>> Truncating Table: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT('>> Inserting Data into : bronze.erp_px_cat_g1v2');	
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Elite 1040 G5\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK);
		SET @end_time=GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time)AS NVARCHAR)+'seconds');
		SET @batch_end_time=GETDATE();

		PRINT('================================');
		PRINT('LOADING DATA INTO BRONZE LAYER COMPLETED')
		PRINT('		-Loading Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR) +'seconds');


	END TRY
	BEGIN CATCH
		PRINT('===========================================');
		PRINT('ERROR OCCURRED DURINF LOADING BRONZE LAYER');
		PRINT('ERROR MESSAGE: '+ ERROR_MESSAGE());
		PRINT('ERROR NUMBER: '+CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('ERROR STATE: '+CAST(ERROR_STATE() AS NVARCHAR));
	END CATCH
	

END

EXEC bronze.load_bronze;
