# 品質報告目錄

此目錄包含自動生成的 Excel 格式資料品質報告。

## 報告格式

**檔名格式**: `data_quality_report_YYYYMMDD_HHMM.xlsx`

## 報告內容

每個 Excel 檔案包含 7 個工作表:

| 工作表名稱 | 說明 |
|-----------|------|
| **Daily_Quality_Metrics** | 每日銷售指標、異常值偵測、品質評分 |
| **Customer_Data_Quality** | 客戶ID完整性分析 |
| **Return_Rate_Analysis** | 退貨率趨勢與異常偵測 |
| **Product_Quality** | 產品資料完整性指標 |
| **Overall_Summary** | 高階統計資料與執行摘要 |
| **Anomaly_Details** | 偵測到的異常詳細清單 |
| **Monthly_Trends** | 月度匯總趨勢與模式 |

## 使用方式

- **商業智慧**: 直接匯入 Tableau、PowerBI 或 Excel
- **品質監控**: 追蹤資料品質的時間趨勢
- **異常偵測**: 識別與調查資料品質問題

## 保留政策

報告在每次 ETL 執行後產生，並保留供歷史分析使用。

## 注意事項

此目錄被 git 忽略，避免大型報告檔案被提交到版本庫中。