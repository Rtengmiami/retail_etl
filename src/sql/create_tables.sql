-- Online Retail II Data Warehouse Schema (Star Schema)
-- PostgreSQL compatible

-- Drop tables if exists (for development)
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;
DROP TABLE IF EXISTS raw_retail_data CASCADE;

-- Raw data staging table
CREATE TABLE raw_retail_data (
    id SERIAL PRIMARY KEY,
    invoice VARCHAR(20),
    stock_code VARCHAR(20),
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    price DECIMAL(10,2),
    customer_id INTEGER,
    country VARCHAR(50),
    created_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours')
);

-- Dimension: Country
CREATE TABLE dim_country (
    country_key SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours'),
    updated_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours')
);

-- Dimension: Time
CREATE TABLE dim_time (
    time_key SERIAL PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,
    year INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours')
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    stock_code VARCHAR(20) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours'),
    updated_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours')
);

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER UNIQUE NOT NULL,
    country_key INTEGER REFERENCES dim_country(country_key),
    created_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours'),
    updated_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours')
);

-- Fact: Sales
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(20) NOT NULL,
    product_key INTEGER REFERENCES dim_product(product_key),
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    country_key INTEGER REFERENCES dim_country(country_key),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(15,2) NOT NULL,
    is_return BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP + INTERVAL '8 hours')
);

-- Create indexes for performance
CREATE INDEX idx_fact_sales_invoice ON fact_sales(invoice_no);
CREATE INDEX idx_fact_sales_time ON fact_sales(time_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_country ON fact_sales(country_key);
CREATE INDEX idx_raw_retail_invoice_date ON raw_retail_data(invoice_date);

-- Add composite unique constraint for deduplication
ALTER TABLE fact_sales ADD CONSTRAINT uk_sales_dedup 
UNIQUE (invoice_no, product_key, time_key);

-- Comments for documentation
COMMENT ON TABLE fact_sales IS 'Main fact table containing sales transactions';
COMMENT ON TABLE dim_product IS 'Product dimension with stock codes and descriptions';
COMMENT ON TABLE dim_customer IS 'Customer dimension with customer IDs and countries';
COMMENT ON TABLE dim_time IS 'Time dimension with date attributes';
COMMENT ON TABLE dim_country IS 'Country dimension';
COMMENT ON TABLE raw_retail_data IS 'Staging table for raw retail data';