# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **standalone Docker-based Airflow ETL Pipeline** for processing UK Online Retail II dataset (2009-2011) into a PostgreSQL data warehouse. The project transforms raw Excel transaction data into a star schema with comprehensive data quality monitoring and automated reporting.

## Architecture

### Core ETL Pipeline
The project follows a modular ETL architecture with clear separation of concerns:

- **Extract** (`src/etl/extract/`): Handles Excel file reading and basic cleaning
- **Transform** (`src/etl/transform/`): Implements star schema transformation with dimension and fact table creation
- **Load** (`src/etl/load/`): Manages data loading and post-load validation
- **Quality Monitoring** (`src/etl/quality_monitoring.py`): Comprehensive data quality framework with Excel report generation

### Data Warehouse Design
Implements a **Star Schema** with:
- **Fact Table**: `fact_sales` (core transaction data)
- **Dimension Tables**: `dim_product`, `dim_customer`, `dim_time`, `dim_country`
- **Staging Table**: `raw_retail_data` (temporary storage)

### Current Configuration
The project is configured as a **standalone Docker environment** with:
- **Import Format**: `from src.*` - Optimized for containerized deployment
- **Container Path**: `/opt/airflow` - Simplified path structure
- **Database Setup**: Dual PostgreSQL containers (Airflow metadata + Retail data warehouse)
- **Executor**: LocalExecutor for lightweight operation

## Common Commands

### Quick Start
```bash
# Setup environment and start all services
source ./setup_env.sh  # Unix/Linux/macOS
./start.sh

# Windows
setup_env.bat
start.bat
```

### Development and Testing
```bash
# Test DAG syntax inside container
docker exec -it retail_etl_standalone-airflow-webserver-1 python /opt/airflow/dags/retail_etl_dag.py

# Generate standalone data quality report
docker exec -it retail_etl_standalone-airflow-webserver-1 python /opt/airflow/scripts/export_quality_data.py

# Check container logs
docker logs retail_etl_standalone-airflow-webserver-1
```

### Alternative Path Management (Legacy)
```bash
# Convert to existing Airflow format (if needed)
./fix_imports_for_option2.sh .

# Revert to standalone format
./fix_imports_for_option1.sh .
```

### Docker Environment
```bash
# Full stack deployment
source ./setup_env.sh && ./start.sh     # Unix/Linux/macOS
setup_env.bat && start.bat               # Windows

# Stop all services
./stop.sh                                # Unix/Linux/macOS
stop.bat                                 # Windows

# Standalone operations (development)
./start_standalone.sh                    # Start individual containers
./stop_standalone.sh                     # Stop individual containers
```

### Database Operations
```bash
# Connect to retail data warehouse
psql -h localhost -p 5433 -U airflow -d retail_dw

# Connect to Airflow metadata database
psql -h localhost -p 5432 -U airflow -d airflow

# Execute SQL scripts inside container
docker exec -it retail_etl_standalone-postgres-retail-1 psql -U airflow -d retail_dw -f /opt/airflow/src/sql/create_tables.sql
```

## Key Configuration

### Environment Configuration

#### Core Settings (`.env`)
```bash
# Database - Retail Data Warehouse
DB_HOST=postgres-retail
DB_PORT=5432
DB_NAME=retail_dw
DB_USER=airflow
DB_PASSWORD=airflow

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# ETL Paths (Container-relative)
DATA_SOURCE_PATH=/opt/airflow/data/raw/online_retail_II.xlsx
BATCH_SIZE=10000
LOG_LEVEL=INFO
```

#### Airflow Variables (Optional Override)
- `retail_source_file`: Override data source path
- `postgres_retail_dw`: Override database connection
- `etl_log_path`: Override log directory

#### Container Architecture
```bash
# Service Mapping
localhost:8080  -> Airflow Web UI
localhost:5433  -> Retail PostgreSQL (postgres-retail)
localhost:5432  -> Airflow PostgreSQL (postgres)

# Volume Mapping
./data     -> /opt/airflow/data
./logs     -> /opt/airflow/logs
./dags     -> /opt/airflow/dags
./src      -> /opt/airflow/src
```

## Data Requirements

### Source Data
- **File**: `online_retail_II.xlsx` (≈45MB)
- **Location**: `data/raw/online_retail_II.xlsx`
- **Source**: UCI Machine Learning Repository
- **Container Path**: `/opt/airflow/data/raw/online_retail_II.xlsx`

### Directory Structure
```
data/
├── raw/                    # Original Excel files
├── processed/              # Intermediate processing files
└── quality_reports/        # Generated DQ reports
```

## ETL Flow

The main DAG (`retail_etl_pipeline`) orchestrates:

```
start_pipeline
     ↓
create_tables → extract_data → transform_data → validate_quality
                                                        ↓
cleanup_staging ← notify_success ← run_quality_monitoring ← run_dq_checks
                                                        ↓
                                                   end_pipeline
```

### Task Details
1. **create_tables**: Initialize PostgreSQL schema (staging + star schema)
2. **extract_data**: Excel → staging table with data cleaning
3. **transform_data**: Staging → star schema (fact_sales + dimensions)
4. **validate_quality**: Comprehensive data quality validation
5. **run_dq_checks**: SQL-based quality checks execution
6. **run_quality_monitoring**: Generate Excel DQ reports
7. **notify_success**: Email notifications (if configured)
8. **cleanup_staging**: Remove old staging data

## Import Strategy

### Container-Optimized Imports
ETL modules use **lazy imports within functions** for optimal container performance:

```python
def extract_retail_data(**context):
    # Function-level imports (not module-level)
    from src.etl.extract.retail_extract import RetailExtractor
    # ... task logic
```

### Path Resolution
```python
# DAG-level path setup
container_project_path = os.environ.get('CONTAINER_PROJECT_PATH', '/opt/airflow')
sys.path.append(container_project_path)
sys.path.append(container_project_path + '/src')
```

### Benefits
- **Fast DAG parsing**: Only imports needed modules during execution
- **Error isolation**: Import failures confined to specific tasks
- **Container compatibility**: Works with Docker volume mounting
- **Memory efficiency**: Reduced memory footprint during parsing

## Quality Monitoring Framework

### Automated DQ Checks
- **Statistical Analysis**: Daily sales patterns and outlier detection
- **Completeness Validation**: Customer ID and critical field monitoring
- **Business Rule Validation**: Return rate anomaly detection
- **Schema Validation**: Data type and constraint verification
- **Trend Analysis**: Historical data comparison

### Report Generation
- **Location**: `data/quality_reports/` (container: `/opt/airflow/data/quality_reports/`)
- **Format**: Multi-sheet Excel reports with visualizations
- **Naming**: `dq_report_YYYYMMDDHHMMSS.xlsx`
- **Integration**: Tableau/PowerBI compatible format

### Quality Metrics Dashboard
- **Overall Score**: Percentage of passed quality checks
- **Processing Statistics**: Rows processed, rejected, duplicated
- **Business KPIs**: Total revenue, customer count, product coverage

## Testing and Validation

### Container-based Testing
```bash
# Test complete pipeline in container
docker exec retail_etl_standalone-airflow-webserver-1 python /opt/airflow/dags/retail_etl_dag.py

# Test individual ETL components
docker exec retail_etl_standalone-airflow-webserver-1 python /opt/airflow/scripts/export_quality_data.py

# Test database connectivity
docker exec retail_etl_standalone-postgres-retail-1 pg_isready -U airflow -d retail_dw
```

### Airflow UI Testing
- **DAG View**: http://localhost:8080 (airflow/airflow)
- **Task Testing**: Use Airflow UI "Test" button for individual tasks
- **Log Monitoring**: Real-time logs available in Airflow UI

### Manual DAG Validation
```bash
# Validate DAG syntax
docker exec retail_etl_standalone-airflow-webserver-1 airflow dags check retail_etl_pipeline

# Test specific task
docker exec retail_etl_standalone-airflow-webserver-1 airflow tasks test retail_etl_pipeline extract_data 2024-01-01
```