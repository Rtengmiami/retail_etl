"""
Data Quality Monitoring Module
Monitors data quality based on actual data warehouse content.
Generates Excel reports for Tableau visualization.
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.utils.timezone_utils import now_taipei_iso

import numpy as np

# Try to import Airflow components (might not be available in local testing)
try:
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.exceptions import AirflowNotFoundException
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

load_dotenv()

logger = logging.getLogger('quality_monitoring')

class DataQualityMonitor:
    def __init__(self):
        # Load environment variables from .env file if in Airflow environment
        self._load_env_file()
        self.engine = self._create_engine()
        # Use absolute path for Airflow environment
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # Go up to project root
        self.report_path = Path(base_path) / "data" / "quality_reports"
        self.report_path.mkdir(parents=True, exist_ok=True)
    
    def _load_env_file(self):
        """Load environment variables from .env file in Airflow environment."""
        # Try to find .env file relative to this module's location
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up from src/etl to project root
        project_root = os.path.dirname(os.path.dirname(current_dir))
        env_path = os.path.join(project_root, '.env')
        
        if os.path.exists(env_path):
            load_dotenv(env_path)
            logger.info(f"Loaded .env from {env_path}")
        else:
            logger.info("No .env file found, using system environment variables")
    
    def _create_engine(self) -> Engine:
        """Create database engine with Airflow Connection fallback to environment variables."""
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
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'name': os.getenv('DB_NAME', 'retail_dw'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
        
        connection_string = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
        logger.info(f"Using environment variables for database connection: {db_config['host']}:{db_config['port']}")
        return create_engine(connection_string)
    
    def get_data_date_range(self) -> dict:
        """Get the date range of data in the warehouse."""
        query = """
            SELECT 
                MIN(t.date_value) as min_date,
                MAX(t.date_value) as max_date,
                COUNT(DISTINCT t.date_value) as total_days
            FROM fact_sales f
            JOIN dim_time t ON f.time_key = t.time_key
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            return {
                'min_date': result.min_date,
                'max_date': result.max_date,
                'total_days': result.total_days
            }
    
    def check_daily_sales_range(self) -> pd.DataFrame:
        """檢查每日銷售總額範圍，識別異常日期."""
        logger.info("Checking daily sales range")
        
        query = """
            SELECT 
                t.date_value,
                t.year,
                t.month,
                t.day_name,
                t.is_weekend,
                SUM(CASE WHEN f.is_return = FALSE THEN f.total_amount ELSE 0 END) as daily_sales,
                COUNT(*) as total_transactions,
                COUNT(DISTINCT f.customer_key) as unique_customers,
                SUM(CASE WHEN f.is_return = TRUE THEN f.total_amount ELSE 0 END) as daily_returns,
                COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) as return_transactions
            FROM fact_sales f
            JOIN dim_time t ON f.time_key = t.time_key
            GROUP BY t.date_value, t.year, t.month, t.day_name, t.is_weekend
            ORDER BY t.date_value
        """
        
        df = pd.read_sql(query, self.engine)
        
        # 計算統計指標
        mean_sales = df['daily_sales'].mean()
        std_sales = df['daily_sales'].std()
        q25 = df['daily_sales'].quantile(0.25)
        q75 = df['daily_sales'].quantile(0.75)
        iqr = q75 - q25
        
        # 異常值檢測（使用 IQR 方法）
        lower_bound = q25 - 1.5 * iqr
        upper_bound = q75 + 1.5 * iqr
        
        # 標記異常
        df['is_outlier'] = (df['daily_sales'] < lower_bound) | (df['daily_sales'] > upper_bound)
        df['outlier_type'] = df.apply(lambda row: 
            'Low' if row['daily_sales'] < lower_bound else
            'High' if row['daily_sales'] > upper_bound else
            'Normal', axis=1)
        
        # 計算品質分數（0-100）
        df['quality_score'] = df.apply(lambda row: 
            85 if row['is_outlier'] else 100, axis=1)
        
        # 添加統計資訊
        df['mean_sales'] = mean_sales
        df['std_sales'] = std_sales
        df['z_score'] = (df['daily_sales'] - mean_sales) / std_sales
        
        return df
    
    def check_missing_customer_rate(self) -> pd.DataFrame:
        """檢查缺失客戶 ID 比例."""
        logger.info("Checking missing customer ID rates")
        
        query = """
            SELECT 
                t.date_value,
                t.year,
                t.month,
                COUNT(*) as total_transactions,
                COUNT(f.customer_key) as transactions_with_customer,
                COUNT(*) - COUNT(f.customer_key) as missing_customers,
                ROUND(
                    (COUNT(*) - COUNT(f.customer_key)) * 100.0 / COUNT(*), 2
                ) as missing_customer_rate
            FROM fact_sales f
            JOIN dim_time t ON f.time_key = t.time_key
            GROUP BY t.date_value, t.year, t.month
            ORDER BY t.date_value
        """
        
        df = pd.read_sql(query, self.engine)
        
        # 設定缺失率閾值
        threshold = 5.0  # 5% 以下為正常
        
        df['quality_status'] = df['missing_customer_rate'].apply(lambda x: 
            'Good' if x <= threshold else
            'Warning' if x <= threshold * 2 else
            'Critical')
        
        df['quality_score'] = df['missing_customer_rate'].apply(lambda x:
            100 if x <= threshold else
            max(50, 100 - (x - threshold) * 10))
        
        return df
    
    def check_return_rate_analysis(self) -> pd.DataFrame:
        """分析退貨率趨勢."""
        logger.info("Analyzing return rate trends")
        
        query = """
            SELECT 
                t.date_value,
                t.year,
                t.month,
                COUNT(*) as total_transactions,
                COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) as return_transactions,
                ROUND(
                    COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0 / COUNT(*), 2
                ) as return_rate,
                SUM(CASE WHEN f.is_return = FALSE THEN f.total_amount ELSE 0 END) as sales_amount,
                SUM(CASE WHEN f.is_return = TRUE THEN ABS(f.total_amount) ELSE 0 END) as return_amount
            FROM fact_sales f
            JOIN dim_time t ON f.time_key = t.time_key
            GROUP BY t.date_value, t.year, t.month
            ORDER BY t.date_value
        """
        
        df = pd.read_sql(query, self.engine)
        
        # 計算退貨率統計
        mean_return_rate = df['return_rate'].mean()
        std_return_rate = df['return_rate'].std()
        
        # 異常退貨率檢測
        df['return_rate_z_score'] = (df['return_rate'] - mean_return_rate) / std_return_rate
        df['is_return_anomaly'] = abs(df['return_rate_z_score']) > 2
        
        df['return_status'] = df['return_rate'].apply(lambda x:
            'Low' if x < mean_return_rate - std_return_rate else
            'High' if x > mean_return_rate + std_return_rate else
            'Normal')
        
        return df
    
    def check_product_data_quality(self) -> pd.DataFrame:
        """檢查產品資料完整性."""
        logger.info("Checking product data quality")
        
        query = """
            SELECT 
                p.stock_code,
                p.description,
                COUNT(f.product_key) as transaction_count,
                SUM(f.total_amount) as total_revenue,
                CASE WHEN p.description IS NULL OR p.description = '' THEN 1 ELSE 0 END as missing_description,
                CASE WHEN LENGTH(p.stock_code) < 3 THEN 1 ELSE 0 END as invalid_stock_code
            FROM dim_product p
            LEFT JOIN fact_sales f ON p.product_key = f.product_key
            GROUP BY p.product_key, p.stock_code, p.description
            ORDER BY total_revenue DESC NULLS LAST
        """
        
        df = pd.read_sql(query, self.engine)
        
        # 計算產品資料品質分數
        df['quality_issues'] = df['missing_description'] + df['invalid_stock_code']
        df['quality_score'] = df['quality_issues'].apply(lambda x:
            100 if x == 0 else
            75 if x == 1 else
            50)
        
        return df
    
    def generate_quality_summary(self) -> dict:
        """產生整體品質摘要."""
        logger.info("Generating quality summary")
        
        # 取得資料範圍
        date_range = self.get_data_date_range()
        
        # 整體統計
        overall_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT f.customer_key) as unique_customers,
                COUNT(DISTINCT f.product_key) as unique_products,
                COUNT(DISTINCT f.time_key) as unique_dates,
                SUM(CASE WHEN f.is_return = FALSE THEN f.total_amount ELSE 0 END) as total_sales,
                SUM(CASE WHEN f.is_return = TRUE THEN ABS(f.total_amount) ELSE 0 END) as total_returns,
                ROUND(COUNT(CASE WHEN f.is_return = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as overall_return_rate
            FROM fact_sales f
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(overall_query)).fetchone()
            
        summary = {
            'report_timestamp': now_taipei_iso(),
            'data_date_range': date_range,
            'total_records': result.total_records,
            'unique_customers': result.unique_customers,
            'unique_products': result.unique_products,
            'unique_dates': result.unique_dates,
            'total_sales': float(result.total_sales) if result.total_sales else 0,
            'total_returns': float(result.total_returns) if result.total_returns else 0,
            'overall_return_rate': float(result.overall_return_rate) if result.overall_return_rate else 0
        }
        
        return summary
    
    def export_to_excel(self) -> str:
        """匯出所有品質檢測結果到 Excel."""
        logger.info("Exporting quality monitoring results to Excel")
        
        # 收集所有資料
        daily_sales = self.check_daily_sales_range()
        missing_customers = self.check_missing_customer_rate()
        return_analysis = self.check_return_rate_analysis()
        product_quality = self.check_product_data_quality()
        summary = self.generate_quality_summary()
        from src.utils.timezone_utils import now_taipei

        # 建立檔案名稱 (使用台北時間 UTC+8)
        timestamp = now_taipei().strftime('%Y%m%d_%H%M')
        filename = f"data_quality_report_{timestamp}.xlsx"
        filepath = self.report_path / filename
        
        # 寫入 Excel
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            # Sheet 1: 每日品質指標
            daily_metrics = daily_sales[['date_value', 'daily_sales', 'total_transactions', 
                                       'unique_customers', 'is_outlier', 'outlier_type', 
                                       'quality_score', 'z_score']].copy()
            daily_metrics.to_excel(writer, sheet_name='Daily_Quality_Metrics', index=False)
            
            # Sheet 2: 客戶資料完整性
            customer_metrics = missing_customers[['date_value', 'total_transactions', 
                                                'missing_customers', 'missing_customer_rate',
                                                'quality_status', 'quality_score']].copy()
            customer_metrics.to_excel(writer, sheet_name='Customer_Data_Quality', index=False)
            
            # Sheet 3: 退貨率分析
            return_metrics = return_analysis[['date_value', 'total_transactions', 
                                            'return_transactions', 'return_rate', 
                                            'return_rate_z_score', 'is_return_anomaly',
                                            'return_status']].copy()
            return_metrics.to_excel(writer, sheet_name='Return_Rate_Analysis', index=False)
            
            # Sheet 4: 產品資料品質
            product_quality_subset = product_quality[['stock_code', 'description', 
                                                    'transaction_count', 'total_revenue',
                                                    'missing_description', 'invalid_stock_code',
                                                    'quality_score']].head(1000)  # 限制行數
            product_quality_subset.to_excel(writer, sheet_name='Product_Quality', index=False)
            
            # Sheet 5: 整體摘要
            summary_df = pd.DataFrame([summary])
            summary_df.to_excel(writer, sheet_name='Overall_Summary', index=False)
            
            # Sheet 6: 異常明細
            anomalies = []
            
            # 銷售異常
            sales_anomalies = daily_sales[daily_sales['is_outlier'] == True]
            for _, row in sales_anomalies.iterrows():
                anomalies.append({
                    'Date': row['date_value'],
                    'Issue_Type': 'Sales Outlier',
                    'Severity': 'Medium' if row['outlier_type'] == 'Low' else 'High',
                    'Value': row['daily_sales'],
                    'Description': f"{row['outlier_type']} sales day: ${row['daily_sales']:,.2f}"
                })
            
            # 退貨異常
            return_anomalies = return_analysis[return_analysis['is_return_anomaly'] == True]
            for _, row in return_anomalies.iterrows():
                anomalies.append({
                    'Date': row['date_value'],
                    'Issue_Type': 'Return Rate Anomaly',
                    'Severity': 'Medium',
                    'Value': row['return_rate'],
                    'Description': f"Unusual return rate: {row['return_rate']}%"
                })
            
            anomalies_df = pd.DataFrame(anomalies)
            if not anomalies_df.empty:
                anomalies_df.to_excel(writer, sheet_name='Anomaly_Details', index=False)
            
            # Sheet 7: 月度趨勢
            monthly_trend = daily_sales.groupby(['year', 'month']).agg({
                'daily_sales': ['sum', 'mean', 'count'],
                'quality_score': 'mean',
                'total_transactions': 'sum'
            }).round(2)
            
            monthly_trend.columns = ['Total_Sales', 'Avg_Daily_Sales', 'Days_Count', 
                                   'Avg_Quality_Score', 'Total_Transactions']
            monthly_trend.reset_index().to_excel(writer, sheet_name='Monthly_Trends', index=False)
        
        logger.info(f"Quality report exported to {filepath}")
        return str(filepath)

def main():
    """主執行函數，用於命令列執行."""
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    monitor = DataQualityMonitor()
    excel_path = monitor.export_to_excel()
    
    print(f"Data Quality Report Generated: {excel_path}")
    print("\nUse this Excel file in Tableau to create quality monitoring dashboards!")

if __name__ == "__main__":
    main()