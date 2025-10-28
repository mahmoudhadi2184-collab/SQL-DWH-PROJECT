/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROC bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME ,
            @batch_start_time DATETIME, @batch_end_time DATETIME

BEGIN TRY
    set @batch_start_time = GETDATE()
    PRINT 'LOADING BRONZE LAYER'
    PRINT '========================================================================'

    PRINT '------------------------------------------------------------------------'
    PRINT 'LOADING CRM TABLES'
    PRINT '------------------------------------------------------------------------'
-----------------

    SET @start_time = GETDATE()
    PRINT '>> Truncate Table : bronze.crm_cust_info'
TRUNCATE TABLE bronze.crm_cust_info 
    
    PRINT '>> Inserting Data Table : bronze.crm_cust_info'
BULK INSERT bronze.crm_cust_info 
FROM 'C:\Queries\Data With Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
        FIRSTROW = 2 ,
        FIELDTERMINATOR = ',' ,
        TABLOCK
)
    SET @end_time = GETDATE()
    PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second , @start_time , @end_time) AS NVARCHAR) + ' Second'
    PRINT '------------------------------------------------------------------------'

-------------------------------

    set @start_time = GETDATE()
    PRINT '>> Truncate Table : bronze.crm_prd_info'
TRUNCATE TABLE bronze.crm_prd_info
    
     PRINT '>> Inserting Data Table : bronze.crm_prd_info'
BULK INSERT bronze.crm_prd_info 
FROM 'C:\Queries\Data With Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
        FIRSTROW = 2 ,
        FIELDTERMINATOR = ',' ,
        TABLOCK
   )
   set @end_time = GETDATE()
   PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second , @start_time , @end_time) AS NVARCHAR) + ' Second'
   PRINT '------------------------------------------------------------------------'

-------------------------------
    
    set @start_time = GETDATE()
    PRINT '>> Truncate Table : bronze.crm_sales_details'
TRUNCATE TABLE bronze.crm_sales_details

    PRINT '>> Inserting Data Table : bronze.crm_sales_details'
BULK INSERT bronze.crm_sales_details
FROM 'C:\Queries\Data With Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
        FIRSTROW = 2 ,
        FIELDTERMINATOR = ',' ,
        TABLOCK
)
    set @end_time = GETDATE()
   PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second , @start_time , @end_time) AS NVARCHAR) + ' Second'
    PRINT '------------------------------------------------------------------------'

-------------------------------

    PRINT '------------------------------------------------------------------------'
    PRINT 'LOADING ERD TABLES'
    PRINT '------------------------------------------------------------------------'

-------------------------------

    set @start_time = GETDATE()
    PRINT '>> Truncate Table : bronze.erd_cust_az12'
TRUNCATE TABLE bronze.erd_cust_az12

    PRINT '>> Inserting Data Table : bronze.erd_cust_az12'
BULK INSERT bronze.erd_cust_az12
FROM 'C:\Queries\Data With Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',' ,
        TABLOCK
)
    set @end_time = GETDATE()
    PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second , @start_time , @end_time) AS NVARCHAR) + ' Second'
    PRINT '------------------------------------------------------------------------'

-------------------------------

    set @start_time = GETDATE()
    PRINT '>> Truncate Table : bronze.erd_loc_a101'
TRUNCATE TABLE bronze.erd_loc_a101

    PRINT '>> Inserting Data Table : bronze.erd_loc_a101'
BULK INSERT bronze.erd_loc_a101
FROM 'C:\Queries\Data With Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',' ,
        TABLOCK
)
    set @end_time = GETDATE()
    PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second , @start_time , @end_time) AS NVARCHAR) + ' Second'
    PRINT '------------------------------------------------------------------------'

-------------------------------

    set @start_time = GETDATE()
    PRINT '>> Truncate Table : bronze.erp_px_cat_g1v2'
TRUNCATE TABLE bronze.erp_px_cat_g1v2

     PRINT '>> Inserting Data Table : bronze.erp_px_cat_g1v2'
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Queries\Data With Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',' ,
        TABLOCK
)
    set @end_time = GETDATE()
    PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second , @start_time , @end_time) AS NVARCHAR) + ' Second'
    PRINT '------------------------------------------------------------------------'
    PRINT 'Loading Bronze Layer Is Completed'
    set @batch_end_time = GETDATE()
    PRINT 'Load Full Duration :' + CAST(DATEDIFF(Second , @batch_start_time ,@batch_end_time) AS NVARCHAR) +' Second'
    PRINT '========================================================================'
  
END TRY
BEGIN CATCH
    PRINT '========================================================================'
    PRINT 'ERROR OCCURATION DURATION LOADING BRONZE LAYER'
    PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE()
    PRINT 'ERROR MESSAGE ' + CAST(ERROR_NUMBER() AS NVARCHAR)
    PRINT 'ERROR MESSAGE ' + CAST(ERROR_STATE() AS NVARCHAR)
    PRINT '========================================================================'
END CATCH
END
