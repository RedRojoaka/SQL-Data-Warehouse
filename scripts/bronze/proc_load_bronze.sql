/*
========================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze) 
========================================================
Purpose:
    This script stored procedure loads data into the "bronze" schema from external CSV files.
    It performs the following:
      - Truncates teh bronze tabels before loading data.
      - Uses the "BULK INSERT" command to load data from csv Files to bronze tables 

Parameters:
    None.
  This stored procedure does not accept any parameters or returns any values.

Usage Example:
    EXEC bronze.load_bronze;
========================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- will help us with optimizing datasets and for making an ETL process
	BEGIN TRY
		SET  @batch_start_time = GETDATE();
		PRINT '========================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================';

		PRINT '------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------';

		SET @start_time = GETDATE();
		-- Using Bulk Insert for all 6 tables 
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- TRUNACATE, ensures table is empty before loading
		-- Excuting this whole clause does whats called as 'Full load'
		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\alexr\Documents\data_warehouse_project_1\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			-- TABLOCK, useful for locking table during load time
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------';

		-- This tests quality of csv and table (good for large files)
		-- Check for data to be in each column and ensure correctness 
		-- Common errors are data is incorrect column name and can be due to definition of table or field seperator
		-- SELECT * FROM bronze.crm_cust_info
		-- Check for count generated to match count of csv (length fo file)
		-- SELECT COUNT(*) FROM bronze.crm_cust_info 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 
		PRINT '>> Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\alexr\Documents\data_warehouse_project_1\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; 
		PRINT '>> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\alexr\Documents\data_warehouse_project_1\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------';

		-- source_erp files
		PRINT '------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\alexr\Documents\data_warehouse_project_1\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\alexr\Documents\data_warehouse_project_1\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\alexr\Documents\data_warehouse_project_1\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------';

		SET @batch_end_time = GETDATE();
		PRINT '===========================================';
		PRINT 'Loading Bronze Layer Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===========================================';
	END TRY
	BEGIN CATCH
		PRINT '============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================';
	END CATCH
END
