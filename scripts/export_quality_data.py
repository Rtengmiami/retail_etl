#!/usr/bin/env python3
"""
Manual Data Quality Monitoring Script
Generates comprehensive data quality Excel report for Tableau visualization.
Can be run independently of Airflow.
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.etl.quality_monitoring import DataQualityMonitor
import logging

def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/quality_monitoring.log')
        ]
    )

def main():
    """Main execution function."""
    print("=" * 70)
    print("Data Quality Monitoring & Excel Report Generation")
    print("=" * 70)
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Create monitor instance
        monitor = DataQualityMonitor()
        
        print("\nStarting data quality analysis...")
        
        # Get data overview
        date_range = monitor.get_data_date_range()
        print(f"\nData Date Range:")
        print(f"   From: {date_range['min_date']}")
        print(f"   To: {date_range['max_date']}")
        print(f"   Total Days: {date_range['total_days']}")
        
        print("\nRunning quality checks...")
        
        # Run individual checks (for progress reporting)
        print("   Checking daily sales range...")
        daily_sales = monitor.check_daily_sales_range()
        outliers = daily_sales[daily_sales['is_outlier'] == True]
        print(f"     Found {len(outliers)} outlier days")
        
        print("   Checking missing customer rates...")
        missing_customers = monitor.check_missing_customer_rate()
        critical_days = missing_customers[missing_customers['quality_status'] == 'Critical']
        print(f"     Found {len(critical_days)} days with critical missing customer rates")
        
        print("   Analyzing return rates...")
        return_analysis = monitor.check_return_rate_analysis()
        return_anomalies = return_analysis[return_analysis['is_return_anomaly'] == True]
        print(f"     Found {len(return_anomalies)} days with unusual return rates")
        
        print("   Checking product data quality...")
        product_quality = monitor.check_product_data_quality()
        quality_issues = product_quality[product_quality['quality_score'] < 100]
        print(f"     Found {len(quality_issues)} products with quality issues")
        
        # Generate comprehensive Excel report
        print("\nGenerating Excel report...")
        excel_path = monitor.export_to_excel()
        
        print(f"\nQuality monitoring completed successfully!")
        print(f"\nReport Location: {Path(excel_path).absolute()}")
        
        # Generate summary statistics
        summary = monitor.generate_quality_summary()
        print(f"\nQuality Summary:")
        print(f"   Total Records: {summary['total_records']:,}")
        print(f"   Unique Customers: {summary['unique_customers']:,}")
        print(f"   Unique Products: {summary['unique_products']:,}")
        print(f"   Unique Dates: {summary['unique_dates']:,}")
        print(f"   Total Sales: ${summary['total_sales']:,.2f}")
        print(f"   Total Returns: ${summary['total_returns']:,.2f}")
        print(f"   Overall Return Rate: {summary['overall_return_rate']:.2f}%")
        
        print("\n" + "=" * 70)
        print("Excel Report Contents:")
        print("=" * 70)
        print("Sheet 1: Daily_Quality_Metrics    - Daily sales patterns & outliers")
        print("Sheet 2: Customer_Data_Quality    - Missing customer ID analysis") 
        print("Sheet 3: Return_Rate_Analysis     - Return rate trends & anomalies")
        print("Sheet 4: Product_Quality          - Product data completeness")
        print("Sheet 5: Overall_Summary          - High-level quality metrics")
        print("Sheet 6: Anomaly_Details          - Detailed anomaly information")
        print("Sheet 7: Monthly_Trends           - Monthly quality trends")
        
        print("\n" + "=" * 70)
        print("Next Steps for Tableau:")
        print("=" * 70)
        print("1. Open Tableau Public Desktop")
        print("2. Connect to the Excel file above")
        print("3. Create dashboards using the different sheets")
        print("4. Focus on anomaly detection and trend analysis")
        print("5. Publish to Tableau Public for sharing")
        
        print("\nRecommended Tableau Visualizations:")
        print("   Daily Sales Trend with Outlier Highlighting")
        print("   Missing Customer Rate Heatmap")
        print("   Return Rate Control Chart")
        print("   Product Quality Score Distribution")
        print("   Monthly Quality Trend Dashboard")
        
        return 0
        
    except Exception as e:
        logger.error(f"Quality monitoring failed: {str(e)}")
        print(f"\nQuality monitoring failed: {str(e)}")
        print("\nTroubleshooting:")
        print("   1. Check database connection in .env file")
        print("   2. Ensure PostgreSQL service is running")
        print("   3. Verify fact_sales table exists and has data")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)