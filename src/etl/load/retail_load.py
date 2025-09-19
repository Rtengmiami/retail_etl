"""
Retail data loading module.
Handles final data loading and post-load validation.
"""

import logging
from typing import Dict, List
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

logger = logging.getLogger('retail_etl.load')

class RetailLoader:
    """Handles final data loading and validation."""
    
    def __init__(self, db_config: Dict = None):
        """Initialize loader with database configuration."""
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
        # Go up from src/etl/load to project root
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
    
    def validate_referential_integrity(self) -> Dict:
        """
        Validate referential integrity between fact and dimension tables.
        
        Returns:
            Validation results
        """
        logger.info("Validating referential integrity")
        
        validation_queries = {
            'orphaned_products': """
                SELECT COUNT(*) FROM fact_sales f 
                LEFT JOIN dim_product p ON f.product_key = p.product_key 
                WHERE p.product_key IS NULL
            """,
            'orphaned_customers': """
                SELECT COUNT(*) FROM fact_sales f 
                LEFT JOIN dim_customer c ON f.customer_key = c.customer_key 
                WHERE c.customer_key IS NULL
            """,
            'orphaned_times': """
                SELECT COUNT(*) FROM fact_sales f 
                LEFT JOIN dim_time t ON f.time_key = t.time_key 
                WHERE t.time_key IS NULL
            """,
            'orphaned_countries': """
                SELECT COUNT(*) FROM fact_sales f 
                LEFT JOIN dim_country co ON f.country_key = co.country_key 
                WHERE co.country_key IS NULL
            """
        }
        
        results = {}
        with self.engine.connect() as conn:
            for check_name, query in validation_queries.items():
                result = conn.execute(text(query)).scalar()
                results[check_name] = result
                if result > 0:
                    logger.warning(f"Found {result} {check_name}")
                else:
                    logger.info(f"No {check_name} found")
        
        return results
    
    def validate_business_rules(self) -> Dict:
        """
        Validate business rules and data quality.
        
        Returns:
            Business rule validation results
        """
        logger.info("Validating business rules")
        
        business_checks = {
            'calculation_errors': """
                SELECT COUNT(*) FROM fact_sales 
                WHERE ABS(total_amount - (quantity * unit_price)) > 0.01
            """,
            'negative_quantities_non_returns': """
                SELECT COUNT(*) FROM fact_sales 
                WHERE quantity < 0 AND is_return = FALSE
            """,
            'zero_unit_price': """
                SELECT COUNT(*) FROM fact_sales 
                WHERE unit_price <= 0
            """,
            'extreme_quantities': """
                SELECT COUNT(*) FROM fact_sales 
                WHERE ABS(quantity) > 10000
            """,
            'future_dates': """
                SELECT COUNT(*) FROM fact_sales f
                JOIN dim_time t ON f.time_key = t.time_key
                WHERE t.date_value > CURRENT_DATE
            """,
            'total_sales_amount': """
                SELECT SUM(total_amount) FROM fact_sales WHERE is_return = FALSE
            """,
            'total_returns_amount': """
                SELECT SUM(ABS(total_amount)) FROM fact_sales WHERE is_return = TRUE
            """
        }
        
        results = {}
        with self.engine.connect() as conn:
            for check_name, query in business_checks.items():
                result = conn.execute(text(query)).scalar()
                results[check_name] = float(result) if result is not None else 0
                logger.info(f"{check_name}: {result}")
        
        return results
    
    def get_data_summary(self) -> Dict:
        """
        Get summary statistics of loaded data.
        
        Returns:
            Data summary statistics
        """
        logger.info("Generating data summary")
        
        summary_queries = {
            'total_transactions': "SELECT COUNT(*) FROM fact_sales",
            'unique_customers': "SELECT COUNT(*) FROM dim_customer",
            'unique_products': "SELECT COUNT(*) FROM dim_product",
            'unique_countries': "SELECT COUNT(*) FROM dim_country",
            'date_range_start': "SELECT MIN(date_value) FROM dim_time",
            'date_range_end': "SELECT MAX(date_value) FROM dim_time",
            'total_revenue': "SELECT SUM(total_amount) FROM fact_sales WHERE is_return = FALSE",
            'total_returns': "SELECT SUM(ABS(total_amount)) FROM fact_sales WHERE is_return = TRUE",
            'return_rate': """
                SELECT ROUND(
                    COUNT(CASE WHEN is_return = TRUE THEN 1 END) * 100.0 / COUNT(*), 2
                ) FROM fact_sales
            """
        }
        
        results = {}
        with self.engine.connect() as conn:
            for metric, query in summary_queries.items():
                result = conn.execute(text(query)).scalar()
                results[metric] = result
        
        logger.info(f"Data summary: {results}")
        return results
    
    def get_top_metrics(self) -> Dict:
        """
        Get top performers metrics.
        
        Returns:
            Top performers data
        """
        logger.info("Calculating top metrics")
        
        metrics = {}
        
        # Top countries by revenue
        top_countries_query = """
            SELECT c.country_name, SUM(f.total_amount) as revenue
            FROM fact_sales f
            JOIN dim_country c ON f.country_key = c.country_key
            WHERE f.is_return = FALSE
            GROUP BY c.country_name
            ORDER BY revenue DESC
            LIMIT 5
        """
        
        # Top products by quantity sold
        top_products_query = """
            SELECT p.stock_code, p.description, SUM(f.quantity) as total_quantity
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.is_return = FALSE
            GROUP BY p.stock_code, p.description
            ORDER BY total_quantity DESC
            LIMIT 10
        """
        
        # Monthly sales trend
        monthly_trend_query = """
            SELECT t.year, t.month, SUM(f.total_amount) as monthly_revenue
            FROM fact_sales f
            JOIN dim_time t ON f.time_key = t.time_key
            WHERE f.is_return = FALSE
            GROUP BY t.year, t.month
            ORDER BY t.year, t.month
        """
        
        with self.engine.connect() as conn:
            # Top countries
            result = conn.execute(text(top_countries_query)).fetchall()
            metrics['top_countries'] = [dict(row._mapping) for row in result]
            
            # Top products
            result = conn.execute(text(top_products_query)).fetchall()
            metrics['top_products'] = [dict(row._mapping) for row in result]
            
            # Monthly trend
            result = conn.execute(text(monthly_trend_query)).fetchall()
            metrics['monthly_trend'] = [dict(row._mapping) for row in result]
        
        return metrics
    
    def run_data_quality_checks(self) -> Dict:
        """
        Run comprehensive data quality checks.
        
        Returns:
            Complete data quality report
        """
        logger.info("Running comprehensive data quality checks")
        
        try:
            # Run all validation checks
            integrity_results = self.validate_referential_integrity()
            business_results = self.validate_business_rules()
            summary_results = self.get_data_summary()
            top_metrics = self.get_top_metrics()
            
            # Compile final report
            dq_report = {
                'validation_timestamp': now_taipei_iso(),
                'referential_integrity': integrity_results,
                'business_rules': business_results,
                'data_summary': summary_results,
                'top_metrics': top_metrics,
                'overall_status': 'PASS'
            }
            
            # Determine overall status
            integrity_issues = sum(integrity_results.values())
            business_issues = sum([
                business_results['calculation_errors'],
                business_results['negative_quantities_non_returns'],
                business_results['future_dates']
            ])

            # Log warnings for non-critical issues
            if business_results['zero_unit_price'] > 0:
                logger.warning(f"Found {business_results['zero_unit_price']} records with zero unit price (promotional items)")
            if business_results['extreme_quantities'] > 0:
                logger.warning(f"Found {business_results['extreme_quantities']} records with extreme quantities (bulk orders)")
            
            if integrity_issues > 0 or business_issues > 0:
                dq_report['overall_status'] = 'FAIL'
                logger.error(f"Data quality issues found: Integrity={integrity_issues}, Business={business_issues}")
            else:
                logger.info("All data quality checks passed")
            
            return dq_report
            
        except Exception as e:
            logger.error(f"Data quality check failed: {e}")
            raise
    
    def cleanup_staging_data(self, keep_days: int = 7) -> Dict:
        """
        Clean up old staging data.
        
        Args:
            keep_days: Number of days of staging data to keep
            
        Returns:
            Cleanup statistics
        """
        logger.info(f"Cleaning up staging data older than {keep_days} days")
        
        cleanup_query = text(f"""
            DELETE FROM raw_retail_data
            WHERE created_at < NOW() - INTERVAL '{keep_days} DAY'
        """)
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(cleanup_query)
                rows_deleted = result.rowcount
                
            logger.info(f"Cleaned up {rows_deleted} rows from staging")
            return {
                'rows_deleted': rows_deleted,
                'cleanup_timestamp': now_taipei_iso()
            }
            
        except Exception as e:
            logger.error(f"Staging cleanup failed: {e}")
            raise


if __name__ == "__main__":
    # Example usage for testing
    import logging.config
    import yaml
    
    # Setup logging
    with open('src/configs/logging.yml', 'r') as f:
        config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    
    loader = RetailLoader()
    
    # Run data quality checks
    dq_report = loader.run_data_quality_checks()
    print(f"Data Quality Report: {dq_report['overall_status']}")
    print(f"Summary: {dq_report['data_summary']}")
    
    # Cleanup staging (optional)
    # cleanup_stats = loader.cleanup_staging_data()
    # print(f"Cleanup stats: {cleanup_stats}")