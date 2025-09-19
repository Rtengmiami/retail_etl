# Quality Reports Directory

This directory contains automatically generated data quality reports in Excel format.

## Report Format

**Filename Pattern**: `data_quality_report_YYYYMMDD_HHMM.xlsx`

## Report Contents

Each Excel file contains 7 worksheets:

| Worksheet | Description |
|-----------|-------------|
| **Daily_Quality_Metrics** | Daily sales metrics, outlier detection, quality scores |
| **Customer_Data_Quality** | Customer ID completeness analysis |
| **Return_Rate_Analysis** | Return rate trends and anomaly detection |
| **Product_Quality** | Product data completeness metrics |
| **Overall_Summary** | High-level statistics and execution summary |
| **Anomaly_Details** | Detailed list of detected anomalies |
| **Monthly_Trends** | Monthly aggregated trends and patterns |

## Usage

- **Business Intelligence**: Import directly into Tableau, PowerBI, or Excel
- **Quality Monitoring**: Track data quality trends over time
- **Anomaly Detection**: Identify and investigate data quality issues

## Retention

Reports are generated after each ETL execution and retained for historical analysis.

## Note

This directory is ignored by git to prevent large report files from being committed to the repository.