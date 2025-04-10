/*
=============================================================
Stored Procedure: Load Bronze Layer (Sourrce -> Bronze)
=============================================================
Author: Jesco Wurm aka. RaptileBytez
Date: 06-04-2025

    Procedure Purpose:
        This stored procedure is used to load data from external CSV files into the 'bronze' tables in the 'Datawarehouse' database.
      Actions performed:
        - Truncates the existing bronze tables.
        - Uses the BULK INSERT command to load data from CSV files into the corresponding tables in the bronze layer.
        
        The procedure is designed to be run in a SQL Server environment.
        
    Parameters:
        None.
        The procedure does not accept any parameters or return any values.

    WARNING:
        Running this script will truncate any existing tables with the same names in the 'bronze' schema,
        which will result in data loss.
        Proceed with caution and ensure you have backups if necessary.
    
    Usage:
        To execute the procedure, use the following command:
        EXEC bronze.load_bronze;
    =============================================================
    */
USE Datawarehouse;
GO
CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
    DECLARE @step_start_time DATETIME, @step_end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==============================================================';
        PRINT 'Loading data into the bronze layer...';
        PRINT '==============================================================';
        PRINT 'Process started at: ' + CAST(@batch_start_time AS NVARCHAR(50));

        PRINT '---------------------------------------------------------------';
        PRINT 'Loading CRM Tables...';
        PRINT '---------------------------------------------------------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_cust_info...';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Loading data into table: bronze.crm_cust_info...';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Coding\Projects\SQLProjects\SQL-DWH\mssql-datawarehouse\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2, -- Skip the header row
            FIELDTERMINATOR = ',', -- Comma as the field terminator
            ROWTERMINATOR = '\n', -- New line as the row terminator
            TABLOCK -- Use TABLOCK for bulk load
        );
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: bronze.crm_cust_info: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';


        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_prd_info...';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Loading data into table: bronze.crm_prd_info...';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Coding\Projects\SQLProjects\SQL-DWH\mssql-datawarehouse\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2, -- Skip the header row
            FIELDTERMINATOR = ',', -- Comma as the field terminator
            ROWTERMINATOR = '\n', -- New line as the row terminator
            TABLOCK -- Use TABLOCK for bulk load
        );
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: bronze.crm_prd_info: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';


        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_sales_details...';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Loading data into table: bronze.crm_sales_details...';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Coding\Projects\SQLProjects\SQL-DWH\mssql-datawarehouse\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2, -- Skip the header row
            FIELDTERMINATOR = ',', -- Comma as the field terminator
            ROWTERMINATOR = '\n', -- New line as the row terminator
            TABLOCK -- Use TABLOCK for bulk load
        );
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: bronze.crm_sales_details: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';


        PRINT '---------------------------------------------------------------';
        PRINT 'Loading ERP Tables...';
        PRINT '---------------------------------------------------------------';
        
        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_cust_az12...';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Loading data into table: bronze.erp_cust_az12...';        
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Coding\Projects\SQLProjects\SQL-DWH\mssql-datawarehouse\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2, -- Skip the header row
            FIELDTERMINATOR = ',', -- Comma as the field terminator
            ROWTERMINATOR = '\n', -- New line as the row terminator
            TABLOCK -- Use TABLOCK for bulk load
        );
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: bronze.erp_cust_az12: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_cust_az13...';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Loading data into table: bronze.erp_cust_az13...';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Coding\Projects\SQLProjects\SQL-DWH\mssql-datawarehouse\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2, -- Skip the header row
            FIELDTERMINATOR = ',', -- Comma as the field terminator
            ROWTERMINATOR = '\n', -- New line as the row terminator
            TABLOCK -- Use TABLOCK for bulk load
        );
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: bronze.erp_loc_a101: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_px_cat_g1v2...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Loading data into table: bronze.erp_px_cat_g1v2...';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Coding\Projects\SQLProjects\SQL-DWH\mssql-datawarehouse\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2, -- Skip the header row
            FIELDTERMINATOR = ',', -- Comma as the field terminator
            ROWTERMINATOR = '\n', -- New line as the row terminator
            TABLOCK -- Use TABLOCK for bulk load
        );
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';
        SET @batch_end_time = GETDATE();
        PRINT '==============================================================';
        PRINT '>> Data loading into the bronze layer completed successfully.';
        PRINT '>> Total load duration for the batch: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '===============================================================';
    END TRY
    BEGIN CATCH
        PRINT '==============================================================';
        PRINT 'Error occurred while loading data into the bronze layer:';
        PRINT 'Error Message:' + ERROR_MESSAGE();
        PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error Severity:' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
        PRINT 'Error State:' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '==============================================================';
    END CATCH
END;