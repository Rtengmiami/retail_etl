"""
Retail data transformation module.
Transforms raw staging data into dimensional model (Star Schema).
"""

import logging
from typing import Dict, List, Tuple
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os
from datetime import datetime
from src.utils.timezone_utils import now_taipei_iso
from dotenv import load_dotenv

# Try to import Airflow components (might not be available in local testing)
try:
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.exceptions import AirflowNotFoundException
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

load_dotenv()

logger = logging.getLogger('retail_etl.transform')

class RetailTransformer:
    """Transforms retail data into star schema dimensions and facts."""
    
    def __init__(self, db_config: Dict = None):
        """Initialize transformer with database configuration."""
        # Load environment variables from .env file if in Airflow environment
        self._load_env_file()
        self.db_config = db_config or self._get_db_config()
        self.engine = self._create_engine()
        
    def _get_db_config(self) -> Dict:
        """Get database configuration from environment."""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'name': os.getenv('DB_NAME', 'retail_dw'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
    
    def _load_env_file(self):
        """Load environment variables from .env file in Airflow environment."""
        # Try to find .env file relative to this module's location
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up from src/etl/transform to project root
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        env_path = os.path.join(project_root, '.env')
        
        if os.path.exists(env_path):
            load_dotenv(env_path)
            logger.info(f"Loaded .env from {env_path}")
        else:
            logger.info("No .env file found, using system environment variables")
    
    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with Airflow Connection fallback to environment variables."""
        # Try to use Airflow connection first
        if AIRFLOW_AVAILABLE:
            try:
                postgres_hook = PostgresHook(postgres_conn_id='postgres_retail_dw')
                # Get connection details from Airflow
                conn = postgres_hook.get_connection('postgres_retail_dw')
                connection_string = (
                    f"postgresql+psycopg2://{conn.login}:{conn.password}"
                    f"@{conn.host}:{conn.port}/{conn.schema}"
                )
                logger.info("Using Airflow Connection for database")
                return create_engine(connection_string)
            except (AirflowNotFoundException, Exception) as e:
                logger.info(f"Airflow connection not available ({e}), falling back to environment variables")
        
        # Fallback to environment variables
        connection_string = (
            f"postgresql+psycopg2://{self.db_config['user']}:"
            f"{self.db_config['password']}@{self.db_config['host']}:"
            f"{self.db_config['port']}/{self.db_config['name']}"
        )
        logger.info(f"Using environment variables for database connection: {self.db_config['host']}:{self.db_config['port']}")
        return create_engine(connection_string)
    
    def load_staging_data(self) -> pd.DataFrame:
        """Load data from staging table."""
        query = """
            SELECT * FROM raw_retail_data
            WHERE customer_id IS NOT NULL
            AND stock_code IS NOT NULL
            AND invoice_date IS NOT NULL
            ORDER BY invoice_date
        """
        
        logger.info("Loading data from staging table")
        df = pd.read_sql(query, self.engine)
        logger.info(f"Loaded {len(df)} rows from staging")
        return df
    
    def transform_time_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create time dimension from invoice dates.
        
        Args:
            df: Staging dataframe
            
        Returns:
            Time dimension dataframe
        """
        logger.info("Transforming time dimension")
        
        # Get unique dates
        dates = pd.to_datetime(df['invoice_date']).dt.date.unique()
        dates_df = pd.DataFrame({'date_value': dates})
        
        # Extract date components
        dates_df['date_value'] = pd.to_datetime(dates_df['date_value'])
        dates_df['year'] = dates_df['date_value'].dt.year
        dates_df['month'] = dates_df['date_value'].dt.month
        dates_df['month_name'] = dates_df['date_value'].dt.month_name()
        dates_df['quarter'] = dates_df['date_value'].dt.quarter
        dates_df['day_of_month'] = dates_df['date_value'].dt.day
        dates_df['day_of_week'] = dates_df['date_value'].dt.dayofweek + 1  # 1=Monday
        dates_df['day_name'] = dates_df['date_value'].dt.day_name()
        dates_df['is_weekend'] = dates_df['day_of_week'].isin([6, 7])
        
        logger.info(f"Created time dimension with {len(dates_df)} records")
        return dates_df
    
    def transform_country_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create country dimension.
        
        Args:
            df: Staging dataframe
            
        Returns:
            Country dimension dataframe
        """
        logger.info("Transforming country dimension")
        
        countries = df['country'].dropna().unique()
        countries_df = pd.DataFrame({
            'country_name': countries
        })
        
        logger.info(f"Created country dimension with {len(countries_df)} records")
        return countries_df
    
    def transform_product_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create product dimension.
        
        Args:
            df: Staging dataframe
            
        Returns:
            Product dimension dataframe
        """
        logger.info("Transforming product dimension")
        
        # Get unique products with descriptions
        products = df[['stock_code', 'description']].drop_duplicates(subset=['stock_code'])
        products = products.dropna(subset=['stock_code'])
        
        # Handle multiple descriptions for same stock code (take the most common one)
        product_descriptions = df.groupby('stock_code')['description'].agg(
            lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]
        ).reset_index()
        
        products_df = pd.DataFrame({
            'stock_code': product_descriptions['stock_code'],
            'description': product_descriptions['description']
        })
        
        logger.info(f"Created product dimension with {len(products_df)} records")
        return products_df
    
    def transform_customer_dimension(self, df: pd.DataFrame, country_keys: Dict) -> pd.DataFrame:
        """
        Create customer dimension with country foreign keys.
        
        Args:
            df: Staging dataframe
            country_keys: Mapping of country_name to country_key
            
        Returns:
            Customer dimension dataframe
        """
        logger.info("Transforming customer dimension")
        
        # Get unique customers
        customers = df[['customer_id', 'country']].drop_duplicates(subset=['customer_id'])
        customers = customers.dropna(subset=['customer_id'])
        
        # Map countries to keys
        customers['country_key'] = customers['country'].map(country_keys)
        
        customers_df = pd.DataFrame({
            'customer_id': customers['customer_id'].astype(int),
            'country_key': customers['country_key']
        })
        
        logger.info(f"Created customer dimension with {len(customers_df)} records")
        return customers_df
    
    def transform_fact_sales(self, df: pd.DataFrame, dimension_keys: Dict) -> pd.DataFrame:
        """
        Create sales fact table with dimension foreign keys.
        
        Args:
            df: Staging dataframe
            dimension_keys: Dictionary with dimension key mappings
            
        Returns:
            Sales fact dataframe
        """
        logger.info("Transforming sales fact table")
        
        # Create a copy for transformation
        fact_df = df.copy()
        
        # Add dimension keys
        fact_df['product_key'] = fact_df['stock_code'].map(dimension_keys['products'])
        fact_df['customer_key'] = fact_df['customer_id'].map(dimension_keys['customers'])
        fact_df['country_key'] = fact_df['country'].map(dimension_keys['countries'])
        
        # Add time key (map date to time_key)
        fact_df['date_only'] = pd.to_datetime(fact_df['invoice_date']).dt.date
        fact_df['time_key'] = fact_df['date_only'].map(dimension_keys['time'])
        
        # Calculate measures
        fact_df['total_amount'] = fact_df['quantity'] * fact_df['price']
        fact_df['is_return'] = (
            fact_df['invoice'].astype(str).str.startswith('C') | 
            (fact_df['quantity'] < 0)
        )
        
        # Select fact columns
        fact_columns = [
            'invoice', 'product_key', 'customer_key', 'time_key', 'country_key',
            'quantity', 'price', 'total_amount', 'is_return'
        ]
        
        sales_fact = fact_df[fact_columns].copy()
        sales_fact = sales_fact.rename(columns={
            'invoice': 'invoice_no',
            'price': 'unit_price'
        })
        
        # Remove rows with missing keys
        before_cleanup = len(sales_fact)
        sales_fact = sales_fact.dropna(subset=['product_key', 'customer_key', 'time_key'])
        after_cleanup = len(sales_fact)

        # Remove duplicates based on unique constraint columns
        before_dedup = len(sales_fact)
        sales_fact = sales_fact.drop_duplicates(subset=['invoice_no', 'product_key', 'time_key'])
        after_dedup = len(sales_fact)

        logger.info(f"Sales fact: {before_cleanup} -> {after_cleanup} rows after key cleanup")
        logger.info(f"Sales fact: {before_dedup} -> {after_dedup} rows after deduplication")
        logger.info(f"Created sales fact table with {len(sales_fact)} records")
        
        return sales_fact
    
    def load_dimension(self, df: pd.DataFrame, table_name: str, 
                      if_exists: str = 'replace') -> Dict:
        """
        Load dimension table to database.
        
        Args:
            df: Dimension dataframe
            table_name: Target table name
            if_exists: How to handle existing table ('replace', 'append')
            
        Returns:
            Load statistics and key mappings
        """
        logger.info(f"Loading {table_name} with {len(df)} records")
        
        try:
            # If replacing, truncate instead of drop to preserve foreign keys
            if if_exists == 'replace':
                with self.engine.connect() as conn:
                    # Check if table exists
                    table_exists = conn.execute(
                        text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='{table_name}')")
                    ).scalar()
                    
                    if table_exists:
                        # Truncate table to preserve structure and foreign keys
                        # DDL operations like TRUNCATE are autocommitted in SQLAlchemy
                        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
                        if_exists = 'append'  # Now append to empty table
            
            # Load to database in batches to reduce memory usage
            batch_size = 5000  # Process 5000 rows at a time
            total_rows = len(df)
            
            logger.info(f"Loading {total_rows} rows in batches of {batch_size}")
            
            # Load data in batches
            for i in range(0, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                batch_df = df.iloc[i:batch_end]
                
                logger.info(f"Loading batch {i//batch_size + 1}: rows {i+1} to {batch_end}")
                
                batch_df.to_sql(table_name, self.engine, if_exists=if_exists, 
                               index=False, method='multi')
                
                # After first batch, switch to append mode
                if if_exists == 'replace':
                    if_exists = 'append'
                
                # Optional: Force garbage collection after each batch
                import gc
                gc.collect()
            
            # Get the key mappings after insert
            key_query = f"SELECT * FROM {table_name}"
            result_df = pd.read_sql(key_query, self.engine)
            
            # Create key mapping based on table type
            key_mapping = {}
            if table_name == 'dim_time':
                key_mapping = dict(zip(pd.to_datetime(result_df['date_value']).dt.date, 
                                     result_df['time_key']))
            elif table_name == 'dim_country':
                key_mapping = dict(zip(result_df['country_name'], 
                                     result_df['country_key']))
            elif table_name == 'dim_product':
                key_mapping = dict(zip(result_df['stock_code'], 
                                     result_df['product_key']))
            elif table_name == 'dim_customer':
                key_mapping = dict(zip(result_df['customer_id'], 
                                     result_df['customer_key']))
            
            logger.info(f"Successfully loaded {table_name}")
            return {
                'table_name': table_name,
                'rows_loaded': len(df),
                'key_mapping': key_mapping
            }
            
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            raise
    
    def load_fact(self, df: pd.DataFrame, table_name: str = 'fact_sales',
                  if_exists: str = 'replace') -> Dict:
        """
        Load fact table to database.
        
        Args:
            df: Fact dataframe
            table_name: Target table name
            if_exists: How to handle existing table
            
        Returns:
            Load statistics
        """
        logger.info(f"Loading {table_name} with {len(df)} records")
        
        try:
            # Load to database in batches to reduce memory usage
            batch_size = 5000  # Process 5000 rows at a time
            total_rows = len(df)
            rows_loaded = 0
            
            logger.info(f"Loading {total_rows} rows in batches of {batch_size}")
            
            # Clear existing data first (only if replacing)
            if if_exists == 'replace' and total_rows > 0:
                with self.engine.begin() as conn:  # Use begin() for auto-commit
                    conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                logger.info(f"Cleared existing data from {table_name}")
                if_exists = 'append'  # Now append to empty table
            
            # Load data in batches
            for i in range(0, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                batch_df = df.iloc[i:batch_end]
                
                logger.info(f"Loading batch {i//batch_size + 1}: rows {i+1} to {batch_end}")
                
                batch_df.to_sql(
                    table_name, 
                    self.engine, 
                    if_exists=if_exists, 
                    index=False, 
                    method="multi"
                )
                
                rows_loaded += len(batch_df)
                
                # Optional: Force garbage collection after each batch
                import gc
                gc.collect()
            
            logger.info(f"Successfully loaded {rows_loaded} rows to {table_name}")
            return {
                'table_name': table_name,
                'rows_loaded': len(df),
                'load_timestamp': now_taipei_iso()
            }
            
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            raise
    
    def run_full_transformation(self) -> Dict:
        """
        Run complete transformation process.
        
        Returns:
            Transformation statistics
        """
        logger.info("Starting full transformation process")
        stats = {}
        
        try:
            # 1. Load staging data
            df_staging = self.load_staging_data()
            stats['staging_rows'] = len(df_staging)
            
            # 2. Transform dimensions
            dim_time = self.transform_time_dimension(df_staging)
            dim_country = self.transform_country_dimension(df_staging)
            dim_product = self.transform_product_dimension(df_staging)
            
            # 3. Load dimensions and get key mappings
            time_stats = self.load_dimension(dim_time, 'dim_time')
            country_stats = self.load_dimension(dim_country, 'dim_country')
            product_stats = self.load_dimension(dim_product, 'dim_product')
            
            # 4. Load customer dimension (needs country keys)
            dim_customer = self.transform_customer_dimension(
                df_staging, country_stats['key_mapping']
            )
            customer_stats = self.load_dimension(dim_customer, 'dim_customer')
            
            # 5. Transform and load fact table
            dimension_keys = {
                'time': time_stats['key_mapping'],
                'countries': country_stats['key_mapping'],
                'products': product_stats['key_mapping'],
                'customers': customer_stats['key_mapping']
            }
            
            fact_sales = self.transform_fact_sales(df_staging, dimension_keys)
            fact_stats = self.load_fact(fact_sales)
            
            # Compile final statistics
            stats.update({
                'dimensions': {
                    'time': time_stats['rows_loaded'],
                    'country': country_stats['rows_loaded'],
                    'product': product_stats['rows_loaded'],
                    'customer': customer_stats['rows_loaded']
                },
                'fact_sales': fact_stats['rows_loaded'],
                'completion_time': now_taipei_iso()
            })
            
            logger.info(f"Transformation completed successfully. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise


if __name__ == "__main__":
    # Example usage for testing
    import logging.config
    import yaml
    
    # Setup logging
    with open('src/configs/logging.yml', 'r') as f:
        config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    
    transformer = RetailTransformer()
    stats = transformer.run_full_transformation()