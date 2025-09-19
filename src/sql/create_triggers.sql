-- Triggers to automatically update 'updated_at' column in UTC+8
-- PostgreSQL compatible

-- Function to update updated_at with UTC+8 timezone
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = DATE_TRUNC('second', CURRENT_TIMESTAMP + INTERVAL '8 hours');
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for tables with updated_at column
CREATE OR REPLACE TRIGGER update_dim_country_updated_at 
    BEFORE UPDATE ON dim_country 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE TRIGGER update_dim_product_updated_at 
    BEFORE UPDATE ON dim_product 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE TRIGGER update_dim_customer_updated_at 
    BEFORE UPDATE ON dim_customer 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Comments
COMMENT ON FUNCTION update_updated_at_column() IS 'Automatically updates updated_at column with current timestamp in UTC+8';