"""
Comprehensive data quality monitoring module.
Implements Great Expectations-style data quality checks.
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta
from src.utils.timezone_utils import now_taipei_iso
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('data_quality')

class DataQualityChecker:
    """Comprehensive data quality checker for retail ETL pipeline."""
    
    def __init__(self, db_config: Dict = None):
        """Initialize data quality checker."""
        self.db_config = db_config or self._get_db_config()
        self.engine = self._create_engine()
        self.quality_rules = self._load_quality_rules()
        
    def _get_db_config(self) -> Dict:
        """Get database configuration from environment."""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'name': os.getenv('DB_NAME', 'retail_dw'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
    
    def _create_engine(self):
        """Create SQLAlchemy engine."""
        connection_string = (
            f"postgresql+psycopg2://{self.db_config['user']}:"
            f"{self.db_config['password']}@{self.db_config['host']}:"
            f"{self.db_config['port']}/{self.db_config['name']}"
        )
        return create_engine(connection_string)
    
    def _load_quality_rules(self) -> Dict:
        """Define data quality rules."""
        return {
            'raw_data_checks': [
                {
                    'name': 'null_critical_fields',
                    'description': 'Check for nulls in critical fields',
                    'query': '''
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_id,
                            COUNT(CASE WHEN stock_code IS NULL THEN 1 END) as null_stock_code,
                            COUNT(CASE WHEN invoice_date IS NULL THEN 1 END) as null_invoice_date
                        FROM raw_retail_data
                    ''',
                    'threshold': 0.05,  # Max 5% null allowed
                    'critical': True
                },
                {
                    'name': 'duplicate_records',
                    'description': 'Check for duplicate records',
                    'query': '''
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(DISTINCT (invoice, stock_code, invoice_date)) as unique_combinations
                        FROM raw_retail_data
                    ''',
                    'threshold': 0.01,  # Max 1% duplicates
                    'critical': False
                },
                {
                    'name': 'data_range_validation',
                    'description': 'Validate data ranges',
                    'query': '''
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT(CASE WHEN price <= 0 THEN 1 END) as invalid_price,
                            COUNT(CASE WHEN quantity > 1000 THEN 1 END) as suspicious_quantity,
                            COUNT(CASE WHEN quantity = 0 THEN 1 END) as zero_quantity
                        FROM raw_retail_data
                    ''',
                    'threshold': 0.1,  # Max 10% invalid values
                    'critical': False
                }
            ],
            'warehouse_checks': [
                {
                    'name': 'referential_integrity',
                    'description': 'Check referential integrity',
                    'query': '''
                        SELECT 
                            (SELECT COUNT(*) FROM fact_sales f 
                             LEFT JOIN dim_product p ON f.product_key = p.product_key 
                             WHERE p.product_key IS NULL) as orphaned_products,
                            (SELECT COUNT(*) FROM fact_sales f 
                             LEFT JOIN dim_customer c ON f.customer_key = c.customer_key 
                             WHERE c.customer_key IS NULL) as orphaned_customers,
                            (SELECT COUNT(*) FROM fact_sales f 
                             LEFT JOIN dim_time t ON f.time_key = t.time_key 
                             WHERE t.time_key IS NULL) as orphaned_times
                    ''',
                    'threshold': 0,  # No orphaned records allowed
                    'critical': True
                },
                {
                    'name': 'business_logic',
                    'description': 'Validate business calculations',
                    'query': '''
                        SELECT 
                            COUNT(*) as total_sales,
                            COUNT(CASE WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 THEN 1 END) as calculation_errors,
                            AVG(total_amount) as avg_transaction_value,
                            COUNT(CASE WHEN total_amount < 0 AND is_return = FALSE THEN 1 END) as negative_non_returns
                        FROM fact_sales
                    ''',
                    'threshold': 0,  # No calculation errors allowed
                    'critical': True
                }
            ],
            'performance_checks': [
                {
                    'name': 'data_freshness',
                    'description': 'Check data freshness',
                    'query': '''
                        SELECT 
                            MAX(created_at) as last_loaded,
                            EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))/3600 as hours_since_last_load
                        FROM raw_retail_data
                    ''',
                    'threshold': 48,  # Data should not be older than 48 hours
                    'critical': False
                },
                {
                    'name': 'row_count_validation',
                    'description': 'Validate expected row counts',
                    'query': '''
                        SELECT 
                            (SELECT COUNT(*) FROM raw_retail_data) as raw_count,
                            (SELECT COUNT(*) FROM fact_sales) as fact_count,
                            (SELECT COUNT(*) FROM dim_customer) as customer_count,
                            (SELECT COUNT(*) FROM dim_product) as product_count
                    ''',
                    'threshold': 0.95,  # At least 95% of raw data should make it to fact
                    'critical': True
                }
            ]
        }
    
    def run_quality_check(self, check: Dict) -> Dict:
        """
        Run a single data quality check.
        
        Args:
            check: Quality check definition
            
        Returns:
            Check result with pass/fail status
        """
        logger.info(f"Running check: {check['name']}")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(check['query'])).fetchone()
                
            # Convert result to dictionary
            if result:
                check_result = dict(result._mapping)
            else:
                check_result = {}
            
            # Evaluate pass/fail based on check type
            passed = self._evaluate_check_result(check, check_result)
            
            return {
                'check_name': check['name'],
                'description': check['description'],
                'results': check_result,
                'passed': passed,
                'critical': check['critical'],
                'threshold': check['threshold'],
                'timestamp': now_taipei_iso()
            }
            
        except Exception as e:
            logger.error(f"Check {check['name']} failed: {e}")
            return {
                'check_name': check['name'],
                'description': check['description'],
                'results': {},
                'passed': False,
                'critical': check['critical'],
                'error': str(e),
                'timestamp': now_taipei_iso()
            }
    
    def _evaluate_check_result(self, check: Dict, result: Dict) -> bool:
        """Evaluate if check passed based on results and thresholds."""
        check_name = check['name']
        threshold = check['threshold']
        
        if check_name == 'null_critical_fields':
            total_rows = result.get('total_rows', 0)
            if total_rows == 0:
                return False
            
            null_rate = (
                result.get('null_customer_id', 0) + 
                result.get('null_stock_code', 0) + 
                result.get('null_invoice_date', 0)
            ) / (total_rows * 3)  # 3 critical fields
            
            return null_rate <= threshold
            
        elif check_name == 'duplicate_records':
            total = result.get('total_records', 0)
            unique = result.get('unique_combinations', 0)
            if total == 0:
                return True
            
            duplicate_rate = (total - unique) / total
            return duplicate_rate <= threshold
            
        elif check_name == 'data_range_validation':
            total_rows = result.get('total_rows', 0)
            if total_rows == 0:
                return False
            
            invalid_rate = (
                result.get('invalid_price', 0) + 
                result.get('suspicious_quantity', 0)
            ) / total_rows
            
            return invalid_rate <= threshold
            
        elif check_name == 'referential_integrity':
            return (
                result.get('orphaned_products', 0) == 0 and
                result.get('orphaned_customers', 0) == 0 and
                result.get('orphaned_times', 0) == 0
            )
            
        elif check_name == 'business_logic':
            return (
                result.get('calculation_errors', 0) == 0 and
                result.get('negative_non_returns', 0) == 0
            )
            
        elif check_name == 'data_freshness':
            hours_old = result.get('hours_since_last_load', 999)
            return hours_old <= threshold
            
        elif check_name == 'row_count_validation':
            raw_count = result.get('raw_count', 0)
            fact_count = result.get('fact_count', 0)
            if raw_count == 0:
                return False
            
            retention_rate = fact_count / raw_count
            return retention_rate >= threshold
        
        return False
    
    def run_all_checks(self) -> Dict:
        """
        Run all data quality checks.
        
        Returns:
            Comprehensive quality report
        """
        logger.info("Starting comprehensive data quality checks")
        
        report = {
            'execution_time': now_taipei_iso(),
            'check_categories': {},
            'summary': {
                'total_checks': 0,
                'passed_checks': 0,
                'failed_checks': 0,
                'critical_failures': 0
            },
            'overall_status': 'UNKNOWN'
        }
        
        # Run checks by category
        for category, checks in self.quality_rules.items():
            category_results = []
            
            for check in checks:
                result = self.run_quality_check(check)
                category_results.append(result)
                
                # Update summary
                report['summary']['total_checks'] += 1
                if result['passed']:
                    report['summary']['passed_checks'] += 1
                else:
                    report['summary']['failed_checks'] += 1
                    if result.get('critical', False):
                        report['summary']['critical_failures'] += 1
            
            report['check_categories'][category] = category_results
        
        # Determine overall status
        if report['summary']['critical_failures'] > 0:
            report['overall_status'] = 'CRITICAL_FAILURE'
        elif report['summary']['failed_checks'] > 0:
            report['overall_status'] = 'WARNING'
        else:
            report['overall_status'] = 'PASS'
        
        logger.info(f"Data quality checks completed: {report['overall_status']}")
        return report
    
    def get_quality_metrics(self) -> Dict:
        """Get key quality metrics for monitoring."""
        metrics_query = '''
            SELECT 
                -- Raw data metrics
                (SELECT COUNT(*) FROM raw_retail_data) as raw_data_count,
                (SELECT COUNT(DISTINCT customer_id) FROM raw_retail_data WHERE customer_id IS NOT NULL) as unique_customers_raw,
                
                -- Warehouse metrics  
                (SELECT COUNT(*) FROM fact_sales) as fact_sales_count,
                (SELECT COUNT(*) FROM dim_customer) as dim_customers_count,
                (SELECT COUNT(*) FROM dim_product) as dim_products_count,
                (SELECT COUNT(*) FROM dim_country) as dim_countries_count,
                
                -- Quality metrics
                (SELECT COUNT(*) FROM fact_sales WHERE is_return = TRUE) as return_transactions,
                (SELECT ROUND(AVG(total_amount), 2) FROM fact_sales WHERE is_return = FALSE) as avg_transaction_value,
                (SELECT SUM(total_amount) FROM fact_sales WHERE is_return = FALSE) as total_revenue,
                
                -- Data lineage metrics
                (SELECT MAX(created_at) FROM raw_retail_data) as last_raw_load,
                (SELECT MAX(created_at) FROM fact_sales) as last_warehouse_load
        '''
        
        with self.engine.connect() as conn:
            result = conn.execute(text(metrics_query)).fetchone()
            
        return dict(result._mapping) if result else {}
    
    def generate_quality_dashboard_data(self) -> Dict:
        """Generate data for quality monitoring dashboard."""
        
        # Run quality checks
        quality_report = self.run_all_checks()
        
        # Get key metrics
        metrics = self.get_quality_metrics()
        
        # Calculate quality score
        total_checks = quality_report['summary']['total_checks']
        passed_checks = quality_report['summary']['passed_checks']
        quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        dashboard_data = {
            'quality_score': round(quality_score, 1),
            'overall_status': quality_report['overall_status'],
            'last_check_time': quality_report['execution_time'],
            'metrics': metrics,
            'check_summary': quality_report['summary'],
            'failed_checks': []
        }
        
        # Extract failed checks for attention
        for category, checks in quality_report['check_categories'].items():
            for check in checks:
                if not check['passed']:
                    dashboard_data['failed_checks'].append({
                        'category': category,
                        'name': check['check_name'],
                        'description': check['description'],
                        'critical': check.get('critical', False)
                    })
        
        return dashboard_data


if __name__ == "__main__":
    # Example usage for testing
    import logging.config
    import yaml
    import json
    
    # Setup logging
    with open('src/configs/logging.yml', 'r') as f:
        config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    
    checker = DataQualityChecker()
    
    # Run all quality checks
    report = checker.run_all_checks()
    print(f"Quality Report Status: {report['overall_status']}")
    print(f"Summary: {json.dumps(report['summary'], indent=2)}")
    
    # Get dashboard data
    dashboard = checker.generate_quality_dashboard_data()
    print(f"Quality Score: {dashboard['quality_score']}%")
    print(f"Failed Checks: {len(dashboard['failed_checks'])}")