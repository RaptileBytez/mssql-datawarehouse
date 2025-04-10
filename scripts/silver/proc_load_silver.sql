/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================
Author: Jesco Wurm aka. RaptileBytez
Date: 10-04-2025

    Procedure Purpose:
        This stored procedure performs the ETL (Extract, Transform, Load) to populate the 'silver' schema tables 
        from the 'bronze' schema in the 'Datawarehouse' database.
      Actions performed:
        - Truncates silver tables.
        - Uses the BULK INSERT command to load data from CSV files into the corresponding tables in the silver layer.
        - Inserts transformed and cleansed data from Bronze tables into Silver tables.
        
    Parameters:
        None.
        The procedure does not accept any parameters or return any values.
    
    WARNING:
        Running this script will truncate any existing tables with the same names in the 'silver' schema,
        which will result in data loss.
        Proceed with caution and ensure you have backups if necessary.
        
    Usage:
        To execute the procedure, use the following command:
        EXEC silver.load_silver;
=============================================================
*/
USE Datawarehouse;
GO
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @step_start_time DATETIME, @step_end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==============================================================';
        PRINT 'Loading data into the silver layer...';
        PRINT '==============================================================';
        PRINT 'Process started at: ' + CAST(@batch_start_time AS NVARCHAR(50));

        PRINT '---------------------------------------------------------------';
        PRINT 'Loading CRM Tables...';
        PRINT '---------------------------------------------------------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_cust_info...';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Loading data into table: silver.crm_cust_info...';  
        INSERT INTO silver.crm_cust_info
        (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a' END AS cst_marital_status, -- Normalize marital status values to readable format
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a' END AS cst_gndr, -- Normalize gender values to readable format
        cst_create_date
    FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        ) AS t
    WHERE flag_last = 1; --Select the most recent record per customer
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table_ silver.crm_cust_info: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_prd_info...';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Loading data into table: silver.crm_prd_info...';

        INSERT INTO silver.crm_prd_info
        (prd_id, prd_key, cat_id, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, --- Extract the category ID from prd_key
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract the product key from prd_key
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost, -- Replace NULL with 0 for prd_cost
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN  'R' THEN 'Road'
            WHEN 'S' THEN 'other Sales'
            WHEN 'T' THEN 'Touring'     
            ELSE 'n/a' END AS prd_line, -- Normalize product line values to readable format
        CAST (prd_start_dt AS DATE) AS prd_start_dt, -- Convert prd_start_dt to DATE type
        CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
    -- Calculate prd_end_dt as one day before the next start date
    FROM bronze.crm_prd_info;

        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: silver.crm_prd_info: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_sales_details...';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Loading data into table: silver.crm_sales_details...';

        INSERT INTO silver.crm_sales_details
        (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL    
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt, -- Convert sls_order_dt to DATE type
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL    
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt, -- Convert sls_ship_dt to DATE type
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL    
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt, -- Convert sls_due_dt to DATE type
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price) 
            ELSE sls_sales END AS sls_sales, -- Calculate sls_sales as quantity * price if sales is NULL or negative
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price END AS sls_price
    -- Calculate sls_price as sales / quantity if price is NULL or negative
    FROM bronze.crm_sales_details;

        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: silver.crm_sales_details: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';

        PRINT '---------------------------------------------------------------';
        PRINT 'Loading ERP Tables...';
        PRINT '---------------------------------------------------------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_cust_az12...';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Loading data into table: silver.erp_cust_az12...';

        INSERT INTO silver.erp_cust_az12
        (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  -- Remove NAS prefix
            ELSE cid
        END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL 
        ELSE bdate
        END bdate, -- Set future birthdates to NULL
        CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'n/a' -- Normalize gender values
        END AS gen
    FROM bronze.erp_cust_az12;
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: silver.erp_cust_az12: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';

        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_loc_a101...';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Loading data into table: silver.erp_loc_a101...';
        
        INSERT INTO silver.erp_loc_a101
        (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
            WHEN UPPER(TRIM(cntry)) IN ('CA', 'CANADA') THEN 'Canada'
            WHEN UPPER(TRIM(cntry)) IN ('FR', 'FRANCE') THEN 'France'
            WHEN UPPER(TRIM(cntry)) IN ('GB', 'UK', 'UNITED KINGDOM') THEN 'United Kingdom'
            WHEN UPPER(TRIM(cntry)) IN ('IT', 'ITALY') THEN 'Italy'
            WHEN UPPER(TRIM(cntry)) IN ('ES', 'SPAIN') THEN 'Spain'
            WHEN UPPER(TRIM(cntry)) IN ('JP', 'JAPAN') THEN 'Japan'
            WHEN UPPER(TRIM(cntry)) IN ('CN', 'CHINA') THEN 'China'
            WHEN UPPER(TRIM(cntry)) IN ('IN', 'INDIA') THEN 'India'
            WHEN UPPER(TRIM(cntry)) IN ('AU', 'AUSTRALIA') THEN 'Australia'
            WHEN UPPER(TRIM(cntry)) IN ('BR', 'BRAZIL') THEN 'Brazil'
            WHEN UPPER(TRIM(cntry)) IN ('MX', 'MEXICO') THEN 'Mexico'
            WHEN UPPER(TRIM(cntry)) IN ('RU', 'RUSSIA') THEN 'Russia'
            WHEN UPPER(TRIM(cntry)) IN ('ZA', 'SOUTH AFRICA') THEN 'South Africa'
            WHEN UPPER(TRIM(cntry)) IN ('NL', 'NETHERLANDS') THEN 'Netherlands'
            WHEN UPPER(TRIM(cntry)) IN ('SE', 'SWEDEN') THEN 'Sweden'
            WHEN UPPER(TRIM(cntry)) IN ('NO', 'NORWAY') THEN 'Norway'
            WHEN UPPER(TRIM(cntry)) IN ('FI', 'FINLAND') THEN 'Finland'
            WHEN UPPER(TRIM(cntry)) IN ('DK', 'DENMARK') THEN 'Denmark'
            WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a' -- Handle NULL or empty values
            ELSE TRIM(cntry) -- Keep original value if not in the list
            END AS cntry
    FROM bronze.erp_loc_a101;
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: silver.erp_loc_a101: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';
        
        SET @step_start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_px_cat_g1v2...';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Loading data into table: silver.erp_px_cat_g1v2...';
        INSERT INTO silver.erp_px_cat_g1v2
        (id, cat, subcat, maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
        SET @step_end_time = GETDATE();
        PRINT '>> Time taken to load data into table: silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(millisecond, @step_start_time, @step_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '>> ------------';
                
        SET @batch_end_time = GETDATE();
        PRINT '==============================================================';
        PRINT '>> Data loading into the silver layer completed successfully.';
        PRINT '>> Total time taken: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
        PRINT '==============================================================';
        END TRY
    BEGIN CATCH
        PRINT '==============================================================';
        PRINT 'Error occurred during data loading into the silver layer.';
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error Severity:' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
        PRINT 'Error State:' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '==============================================================';
    END CATCH
END;
