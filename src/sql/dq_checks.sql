-- Data Quality Checks for Online Retail II ETL
-- PostgreSQL compatible

-- Check 1: Null values in critical fields
SELECT 
    'Critical Null Check' AS check_name,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) AS null_customer_id,
    COUNT(CASE WHEN stock_code IS NULL THEN 1 END) AS null_stock_code,
    COUNT(CASE WHEN invoice_date IS NULL THEN 1 END) AS null_invoice_date,
    COUNT(CASE WHEN price IS NULL THEN 1 END) AS null_price,
    ROUND(COUNT(CASE WHEN customer_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS null_customer_pct
FROM raw_retail_data;

-- Check 2: Duplicate records detection
SELECT 
    'Duplicate Check' AS check_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT (invoice, stock_code, invoice_date)) AS unique_combinations,
    COUNT(*) - COUNT(DISTINCT (invoice, stock_code, invoice_date)) AS duplicates
FROM raw_retail_data;

-- Check 3: Data range validations
SELECT 
    'Range Validation' AS check_name,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN price <= 0 THEN 1 END) AS invalid_price,
    COUNT(CASE WHEN quantity > 1000 THEN 1 END) AS suspicious_quantity,
    COUNT(CASE WHEN quantity < 0 THEN 1 END) AS negative_quantity,
    COUNT(CASE WHEN LENGTH(stock_code) > 20 THEN 1 END) AS long_stock_code
FROM raw_retail_data;

-- Check 4: Return transactions detection
SELECT 
    'Return Analysis' AS check_name,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN invoice LIKE 'C%' THEN 1 END) AS return_invoices,
    COUNT(CASE WHEN quantity < 0 THEN 1 END) AS negative_quantity,
    ROUND(COUNT(CASE WHEN invoice LIKE 'C%' THEN 1 END) * 100.0 / COUNT(*), 2) AS return_percentage
FROM raw_retail_data;

-- Check 5: Date range validation
SELECT 
    'Date Range Check' AS check_name,
    MIN(invoice_date) AS earliest_date,
    MAX(invoice_date) AS latest_date,
    COUNT(CASE WHEN invoice_date < '2009-01-01' OR invoice_date > '2012-01-01' THEN 1 END) AS out_of_range_dates
FROM raw_retail_data;

-- Check 6: Country data validation
SELECT 
    'Country Validation' AS check_name,
    COUNT(DISTINCT country) AS unique_countries,
    COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) AS null_empty_country,
    COUNT(*) AS total_records,
    ROUND(COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) * 100.0 / COUNT(*), 2) AS null_empty_percentage
FROM raw_retail_data;

-- Check 7: Top countries by transaction volume
SELECT 
    country,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM raw_retail_data 
WHERE country IS NOT NULL
GROUP BY country
ORDER BY transaction_count DESC
LIMIT 10;

-- Check 8: Referential integrity check (after ETL)
SELECT 
    'Referential Integrity' AS check_name,
    (SELECT COUNT(*) FROM fact_sales) AS fact_records,
    (SELECT COUNT(*) FROM fact_sales f 
     LEFT JOIN dim_product p ON f.product_key = p.product_key 
     WHERE p.product_key IS NULL) AS orphaned_products,
    (SELECT COUNT(*) FROM fact_sales f 
     LEFT JOIN dim_customer c ON f.customer_key = c.customer_key 
     WHERE c.customer_key IS NULL) AS orphaned_customers,
    (SELECT COUNT(*) FROM fact_sales f 
     LEFT JOIN dim_time t ON f.time_key = t.time_key 
     WHERE t.time_key IS NULL) AS orphaned_times;

-- Check 9: Business logic validation
SELECT 
    'Business Logic Check' AS check_name,
    COUNT(*) AS total_sales,
    COUNT(CASE WHEN total_amount != quantity * unit_price THEN 1 END) AS calculation_errors,
    AVG(total_amount) AS avg_transaction_value,
    MAX(total_amount) AS max_transaction_value,
    MIN(total_amount) AS min_transaction_value
FROM fact_sales;

-- Check 10: Data freshness check
SELECT
    'Data Freshness' AS check_name,
    MAX(created_at) AS last_loaded,
    EXTRACT(EPOCH FROM ((CURRENT_TIMESTAMP + INTERVAL '8 hours') - MAX(created_at)))/3600 AS hours_since_last_load
FROM raw_retail_data;