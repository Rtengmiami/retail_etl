"""
Retail data extraction module.
Handles reading from Excel files and basic data validation.
"""

import os
import logging
from typing import Dict, Optional, Tuple
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Try to import Airflow components (might not be available in local testing)
try:
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.exceptions import AirflowNotFoundException
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

load_dotenv()

logger = logging.getLogger("retail_etl.extract")


class RetailExtractor:
    """Extracts retail data from Excel source."""

    # Deduplication keys as constant for consistency (after column renaming)
    DEDUP_KEYS = ["InvoiceNo", "StockCode", "InvoiceDate"]

    def __init__(self, db_config: Optional[Dict] = None):
        """Initialize extractor with database configuration."""
        # Load environment variables from .env file if in Airflow environment
        self._load_env_file()
        self.db_config = db_config or self._get_db_config()
        self.engine = self._create_engine()

    def _get_db_config(self) -> Dict:
        """Get database configuration from environment."""
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "name": os.getenv("DB_NAME", "retail_dw"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
        }

    def _load_env_file(self):
        """Load environment variables from .env file in Airflow environment."""
        # Try to find .env file relative to this module's location
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up from src/etl/extract to project root
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

    def extract_from_excel(self, file_path: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Extract data from Excel file with validation.

        Args:
            file_path: Path to Excel file

        Returns:
            Tuple of (cleaned_dataframe, extraction_stats)
        """
        logger.info(f"Starting extraction from {file_path}")

        # Read Excel file
        try:
            df_raw = pd.read_excel(file_path)
            logger.info(f"Successfully read {len(df_raw)} rows from Excel")
        except Exception as e:
            logger.error(f"Failed to read Excel file: {e}")
            raise

        # Basic statistics
        initial_count = len(df_raw)

        # Standardize column names (handle space in 'Customer ID')
        column_mapping = {
            "Customer ID": "CustomerID",
            "Invoice": "InvoiceNo",
            "Price": "UnitPrice",
        }
        df_raw = df_raw.rename(columns=column_mapping)

        # Data quality checks and cleaning
        df_clean, stats = self._clean_data(df_raw)

        # Final statistics
        stats.update(
            {
                "initial_rows": initial_count,
                "final_rows": len(df_clean),
                "rows_dropped": initial_count - len(df_clean),
                "drop_rate": round(
                    (initial_count - len(df_clean)) / initial_count * 100, 2
                ),
            }
        )

        logger.info(f"Extraction completed. Stats: {stats}")
        return df_clean, stats

    def _clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Clean and validate data.

        Args:
            df: Raw dataframe

        Returns:
            Tuple of (cleaned_dataframe, cleaning_stats)
        """
        stats = {}
        df_work = df.copy()

        # Check for required columns
        required_cols = ["InvoiceNo", "StockCode", "InvoiceDate", "CustomerID"]
        missing_cols = [col for col in required_cols if col not in df_work.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Remove rows with null values in critical fields
        initial_count = len(df_work)
        df_work = df_work.dropna(subset=["CustomerID", "StockCode", "InvoiceDate"])
        stats["null_drops"] = initial_count - len(df_work)

        # Handle price validation
        invalid_price_mask = (df_work["UnitPrice"] <= 0) | df_work["UnitPrice"].isna()
        stats["invalid_price_count"] = invalid_price_mask.sum()
        # Keep invalid prices but flag them
        df_work["price_flag"] = invalid_price_mask

        # Handle suspicious quantities
        high_qty_mask = df_work["Quantity"] > 1000
        stats["high_quantity_count"] = high_qty_mask.sum()
        df_work["quantity_flag"] = high_qty_mask

        # Detect returns
        return_mask = (df_work["InvoiceNo"].astype(str).str.startswith("C")) | (
            df_work["Quantity"] < 0
        )
        stats["return_count"] = return_mask.sum()
        df_work["is_return"] = return_mask

        # Remove duplicates
        before_dedup = len(df_work)
        df_work = df_work.drop_duplicates(subset=self.DEDUP_KEYS)
        stats["duplicates_removed"] = before_dedup - len(df_work)

        # Calculate total amount
        df_work["TotalAmount"] = df_work["Quantity"] * df_work["UnitPrice"]

        # Add processing timestamp
        df_work["processed_at"] = pd.Timestamp.now()

        return df_work, stats

    def load_to_staging(
        self, df: pd.DataFrame, table_name: str = "raw_retail_data"
    ) -> Dict:
        """
        Load dataframe to staging table.

        Args:
            df: Cleaned dataframe
            table_name: Target staging table name

        Returns:
            Load statistics
        """
        logger.info(f"Loading {len(df)} rows to staging table: {table_name}")

        try:
            # Map DataFrame columns to database columns
            column_mapping = {
                "InvoiceNo": "invoice",
                "StockCode": "stock_code",
                "Description": "description",
                "Quantity": "quantity",
                "InvoiceDate": "invoice_date",
                "UnitPrice": "price",
                "CustomerID": "customer_id",
                "Country": "country",
            }

            df_db = df.rename(columns=column_mapping)

            # Select only columns that exist in the database
            db_columns = [
                "invoice",
                "stock_code",
                "description",
                "quantity",
                "invoice_date",
                "price",
                "customer_id",
                "country",
            ]
            df_db = df_db[db_columns]

            # Load to database in batches to reduce memory usage
            batch_size = 5000  # Process 5000 rows at a time
            total_rows = len(df_db)
            rows_loaded = 0
            
            logger.info(f"Loading {total_rows} rows in batches of {batch_size}")
            
            # Clear existing data first (only on first batch)
            if total_rows > 0:
                # Truncate table first to ensure clean load
                with self.engine.begin() as conn:  # Use begin() for auto-commit
                    conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                logger.info(f"Cleared existing data from {table_name}")
            
            # Load data in batches
            for i in range(0, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                batch_df = df_db.iloc[i:batch_end]
                
                logger.info(f"Loading batch {i//batch_size + 1}: rows {i+1} to {batch_end}")
                
                # Use 'append' for all batches since we cleared the table above
                batch_rows = batch_df.to_sql(
                    table_name, 
                    self.engine, 
                    if_exists="append", 
                    index=False, 
                    method="multi"
                )
                
                rows_loaded += len(batch_df)
                
                # Optional: Force garbage collection after each batch
                import gc
                gc.collect()

            logger.info(f"Successfully loaded {rows_loaded} rows to {table_name}")

            return {
                "table_name": table_name,
                "rows_loaded": len(df_db),
                "load_timestamp": pd.Timestamp.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to load data to staging: {e}")
            raise

    def get_extraction_summary(self) -> Dict:
        """Get summary of last extraction from staging table."""
        query = text(
            """
            SELECT 
                COUNT(*) as total_rows,
                MIN(invoice_date) as earliest_date,
                MAX(invoice_date) as latest_date,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT stock_code) as unique_products,
                COUNT(DISTINCT country) as unique_countries,
                MAX(created_at) as last_load_time
            FROM raw_retail_data
        """
        )

        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()

        return dict(result._mapping) if result else {}


if __name__ == "__main__":
    # Example usage for testing
    import logging.config
    import yaml

    # Setup logging
    with open("src/configs/logging.yml", "r") as f:
        config = yaml.safe_load(f)
        logging.config.dictConfig(config)

    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    extractor = RetailExtractor()
    source_path = os.getenv('DATA_SOURCE_PATH', 'data/raw/online_retail_II.xlsx')
    df_clean, stats = extractor.extract_from_excel(source_path)

    # Load to staging
    load_stats = extractor.load_to_staging(df_clean)
    print(f"Load stats: {load_stats}")
