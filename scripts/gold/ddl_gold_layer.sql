USE Datawarehouse;
/*
===================================================================================
DDL Script: Create Gold Views
=====================================================================================
Script Purpose:
    This script creates views in the 'gold' schema of the 'Datawarehouse' database.
    The Gold layer represents the final dimenson and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched and business-ready dataset.

Usage:
    These views can be queried directly for analytics and reporting.
=====================================================================================
*/
-- ======================================================================================
-- Create Dimension: gold.dim_customers
-- ======================================================================================
GO
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cci.cst_id) AS customer_key, -- Surrogate key for the customer
    cci.cst_id AS customer_id,
    cci.cst_key AS customer_number,
    cci.cst_firstname AS first_name,
    cci.cst_lastname AS last_name,
        ecl.cntry AS country,
    cci.cst_marital_status AS marital_status,
    CASE WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr -- CRM table is the master for gender        
        ELSE  COALESCE(eci.gen,'n/a')
    END AS gender,
    eci.bdate AS birthdate,
    cci.cst_create_date AS create_date
    FROM silver.crm_cust_info cci
    LEFT JOIN silver.erp_cust_az12 eci
        ON cci.cst_key = eci.cid
    LEFT JOIN silver.erp_loc_a101 ecl
        ON cci.cst_key = ecl.cid;

-- ======================================================================================
-- Create Dimension: gold.dim_products view
-- ======================================================================================
GO
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cpi.prd_start_dt, cpi.prd_key) AS product_key, -- Surrogate key for the product
    cpi.prd_id AS product_id,
    cpi.prd_key AS product_number,
    cpi.prd_nm AS product_name,
    cpi.cat_id AS category_id, 
    epc.cat AS category,
    epc.subcat AS subcategory,
    epc.maintenance,
    cpi.prd_cost AS cost,
    cpi.prd_line AS product_line,
    cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epc
    ON cpi.cat_id = epc.id
WHERE cpi.prd_end_dt IS NULL; -- Filter for current products only

-- ======================================================================================
-- Create Fact gold.fact_sales
-- ======================================================================================
GO
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT
    csd.sls_ord_num AS order_number,
    dp.product_key, 
    dc.customer_key,
    csd.sls_order_dt AS order_date,
    csd.sls_ship_dt AS shipping_date,
    csd.sls_due_dt AS due_date,
    csd.sls_sales AS sales_amount,
    csd.sls_quantity AS quantity,
    csd.sls_price AS price
 FROM silver.crm_sales_details csd 
 LEFT JOIN gold.dim_products dp
    ON csd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc
    ON csd.sls_cust_id = dc.customer_id;
