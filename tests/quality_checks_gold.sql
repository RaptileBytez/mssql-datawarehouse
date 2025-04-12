/*
========================================================================================
Quality Checks for Gold Layer
========================================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'gold' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Data model connectivity between fact and dimension tables.
    - Data integrity between related fields.
Usage Notes:
    - Run these checks after data loading Gold Layer.
    - Investigate and resolve any discrepancies found during the checks.
========================================================================================
*/
USE Datawarehouse;
-- =======================================================================================
-- Checking 'gold.dim_customers'
-- =======================================================================================
-- Check for Uniqueness of Product Key in gold.dim_customers
-- Expecting 0 rows
SELECT
    customer_key, 
    COUNT(*) AS dublicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- =======================================================================================
-- Checking 'gold.dim_products'
-- =======================================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expecting 0 rows
SELECT
    product_key, 
    COUNT(*) AS dublicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
  

-- ======================================================================================
-- Checking 'gold.fact_sales'
-- ======================================================================================
-- Check the data model connectivity between the fact and dimensions
-- Expecting 0 rows
SELECT *
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
WHERE dc.customer_key IS NULL OR dp.product_key IS NULL;