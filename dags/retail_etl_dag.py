"""
Airflow DAG for Online Retail II ETL Pipeline.
Orchestrates extraction, transformation, and loading of retail data.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import logging
import sys

# Add project path to system path for imports
# Use environment variable for container project path
container_project_path = os.environ.get('CONTAINER_PROJECT_PATH', '/opt/airflow')
sys.path.append(container_project_path)
sys.path.append(container_project_path + '/src')  # Add src subdirectory for module imports

# Default arguments for the DAG
default_args = {
    'owner': 'Auston',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Online Retail II Dataset',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['etl', 'retail', 'data-warehouse'],
)

def extract_retail_data(**context):
    """Extract data from Excel source."""
    from src.etl.extract.retail_extract import RetailExtractor
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data extraction")
    
    try:
        # Get file path from Airflow Variable or use relative path
        default_path = f'{container_project_path}/data/raw/online_retail_II.xlsx'
        source_file = Variable.get("retail_source_file", default_path)
        
        extractor = RetailExtractor()
        df_clean, stats = extractor.extract_from_excel(source_file)
        
        # Load to staging
        load_stats = extractor.load_to_staging(df_clean)
        
        # Push stats to XCom for downstream tasks
        context['task_instance'].xcom_push(key='extraction_stats', value=stats)
        context['task_instance'].xcom_push(key='load_stats', value=load_stats)
        
        logger.info(f"Extraction completed: {stats}")
        return f"Successfully extracted {stats['final_rows']} rows"
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

def transform_retail_data(**context):
    """Transform data into star schema."""
    from src.etl.transform.retail_transform import RetailTransformer
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data transformation")
    
    try:
        transformer = RetailTransformer()
        stats = transformer.run_full_transformation()
        
        # Push stats to XCom
        context['task_instance'].xcom_push(key='transformation_stats', value=stats)
        
        logger.info(f"Transformation completed: {stats}")
        return f"Successfully transformed data into star schema"
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def validate_data_quality(**context):
    """Validate data quality and business rules."""
    from src.etl.load.retail_load import RetailLoader
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data quality validation")
    
    try:
        loader = RetailLoader()
        dq_report = loader.run_data_quality_checks()
        
        # Push report to XCom
        context['task_instance'].xcom_push(key='dq_report', value=dq_report)
        
        # Check if validation passed
        if dq_report['overall_status'] != 'PASS':
            raise Exception(f"Data quality validation failed: {dq_report}")
        
        logger.info("Data quality validation passed")
        return "Data quality validation completed successfully"
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {e}")
        raise

def send_success_notification(**context):
    """Send success notification with pipeline stats."""
    
    # Get stats from XCom
    extraction_stats = context['task_instance'].xcom_pull(
        task_ids='extract_data', key='extraction_stats'
    )
    transformation_stats = context['task_instance'].xcom_pull(
        task_ids='transform_data', key='transformation_stats'
    )
    dq_report = context['task_instance'].xcom_pull(
        task_ids='validate_quality', key='dq_report'
    )
    
    # Format notification message
    message = f"""
    === Retail ETL Pipeline Success Summary ===

    Pipeline Run: {context['ds']}
    Execution ID: {context['run_id']}

    EXTRACTION PHASE:
    - Records Processed: {extraction_stats.get('final_rows', 'N/A')}
    - Data Drop Rate: {extraction_stats.get('drop_rate', 'N/A')}%

    TRANSFORMATION PHASE:
    - Fact Table Records: {transformation_stats.get('fact_sales', 'N/A')}
    - Dimension Tables: {len(transformation_stats.get('dimensions', {}))}
    - Time Range: {dq_report.get('data_summary', {}).get('date_range_start', 'N/A')} to {dq_report.get('data_summary', {}).get('date_range_end', 'N/A')}

    DATA QUALITY STATUS: {dq_report.get('overall_status', 'N/A')}
    - Total Revenue: ${dq_report.get('data_summary', {}).get('total_revenue', 'N/A'):,.2f}
    - Return Rate: {dq_report.get('data_summary', {}).get('return_rate', 'N/A')}%
    - Unique Customers: {dq_report.get('data_summary', {}).get('unique_customers', 'N/A'):,}
    - Unique Products: {dq_report.get('data_summary', {}).get('unique_products', 'N/A'):,}
    - Countries Covered: {dq_report.get('data_summary', {}).get('unique_countries', 'N/A')}

    TOP PERFORMER:
    - Best Country: {dq_report.get('top_metrics', {}).get('top_countries', [{}])[0].get('country_name', 'N/A')} (${dq_report.get('top_metrics', {}).get('top_countries', [{}])[0].get('revenue', 'N/A'):,.2f})
    """
    
    return message

def cleanup_old_data(**context):
    """Clean up old staging data."""
    from src.etl.load.retail_load import RetailLoader
    
    logger = logging.getLogger(__name__)
    logger.info("Starting staging data cleanup")
    
    try:
        loader = RetailLoader()
        cleanup_stats = loader.cleanup_staging_data(keep_days=7)
        
        logger.info(f"Cleanup completed: {cleanup_stats}")
        return f"Cleaned up {cleanup_stats['rows_deleted']} old records"
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        # Don't fail the pipeline for cleanup issues
        return "Cleanup had issues but pipeline continues"

# Task definitions
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Create database tables if not exist
def create_tables(**context):
    """Create database tables using SQL script."""
    import os
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.exceptions import AirflowNotFoundException
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    env_path = f'{container_project_path}/.env'
    if os.path.exists(env_path):
        load_dotenv(env_path)
    
    # Read SQL file content
    # Use project root relative path for SQL files
    sql_file_path = os.path.join(container_project_path, 'src', 'sql', 'create_tables.sql')
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Try to use Airflow connection first, fallback to environment variables
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_retail_dw')
    except AirflowNotFoundException:
        # Fallback to environment variables
        postgres_hook = PostgresHook(
            schema=os.getenv('DB_NAME', 'retail_dw'),
            login=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432'))
        )
    
    postgres_hook.run(sql_content)
    return "Tables created successfully"

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

# Extract data from source
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_retail_data,
    dag=dag,
)

# Transform data into star schema
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_retail_data,
    dag=dag,
)

# Validate data quality
validate_task = PythonOperator(
    task_id='validate_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Run DQ checks using SQL
def run_dq_checks(**context):
    """Run data quality checks using SQL script."""
    import os
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.exceptions import AirflowNotFoundException
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    env_path = f'{container_project_path}/.env'
    if os.path.exists(env_path):
        load_dotenv(env_path)
    
    # Read SQL file content
    # Use project root relative path for SQL files
    sql_file_path = os.path.join(container_project_path, 'src', 'sql', 'dq_checks.sql')
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Try to use Airflow connection first, fallback to environment variables
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_retail_dw')
    except AirflowNotFoundException:
        # Fallback to environment variables
        postgres_hook = PostgresHook(
            schema=os.getenv('DB_NAME', 'retail_dw'),
            login=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432'))
        )
    
    results = postgres_hook.get_records(sql_content)

    # Log detailed DQ check results
    logging.info("=== Data Quality Check Results ===")
    for result in results:
        logging.info(f"Check Result: {result}")

    return f"Data quality checks completed - {len(results)} checks executed"

run_dq_checks_task = PythonOperator(
    task_id='run_dq_checks',
    python_callable=run_dq_checks,
    dag=dag,
)

# Data Quality Monitoring
def run_quality_monitoring(**context):
    """Execute comprehensive data quality monitoring and generate Excel report."""
    from src.etl.quality_monitoring import DataQualityMonitor
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data quality monitoring")
    
    try:
        monitor = DataQualityMonitor()
        excel_path = monitor.export_to_excel()
        
        logger.info(f"Quality monitoring completed. Report: {excel_path}")
        return {'excel_report_path': excel_path}
        
    except Exception as e:
        logger.error(f"Quality monitoring failed: {e}")
        raise

quality_monitoring_task = PythonOperator(
    task_id='run_quality_monitoring',
    python_callable=run_quality_monitoring,
    dag=dag,
)

# Send success notification
notify_success_task = PythonOperator(
    task_id='notify_success',
    python_callable=send_success_notification,
    dag=dag,
)

# Clean up old staging data
cleanup_task = PythonOperator(
    task_id='cleanup_staging',
    python_callable=cleanup_old_data,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define task dependencies
start_task >> create_tables_task >> extract_task
extract_task >> transform_task >> validate_task
validate_task >> run_dq_checks_task >> quality_monitoring_task
quality_monitoring_task >> notify_success_task >> cleanup_task >> end_task

# Email notification on failure
def failure_callback(context):
    """Send email notification on DAG failure."""
    subject = f"Retail ETL Pipeline FAILED - {context['ds']}"
    body = f"""
    The Retail ETL Pipeline failed on {context['ds']}.
    
    Task: {context['task_instance'].task_id}
    DAG: {context['dag'].dag_id}
    Execution Time: {context['ts']}
    
    Please check the Airflow logs for more details.
    """
    
    # This would send email if SMTP is configured
    logging.error(f"Pipeline failed: {body}")

# Set failure callback for the DAG
dag.on_failure_callback = failure_callback