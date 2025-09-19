# 日誌目錄

此目錄包含 Airflow ETL 管線產生的應用程式日誌。

## 日誌類型

- **Airflow 排程器日誌**: 任務排程與執行記錄
- **Airflow 網頁伺服器日誌**: Web UI 存取與操作記錄
- **DAG 執行日誌**: 個別任務與 DAG 運行記錄
- **ETL 處理日誌**: 資料處理與轉換記錄
- **錯誤日誌**: 例外狀況與錯誤追蹤

## 日誌結構

```
logs/
├── dag_id=retail_etl_pipeline/
│   ├── run_id=manual_YYYY-MM-DD/
│   │   ├── task_id=extract_data/
│   │   ├── task_id=transform_data/
│   │   └── task_id=validate_quality/
│   └── ...
├── scheduler/
└── webserver/
```

## 日誌等級

- **DEBUG**: 詳細診斷資訊
- **INFO**: 一般操作訊息
- **WARNING**: 不會停止執行的潛在問題
- **ERROR**: 可能影響操作的錯誤狀況
- **CRITICAL**: 可能停止應用程式的嚴重錯誤

## 監控方式

日誌會自動輪轉，可透過以下方式查看：
- Airflow Web UI (Graph View → Task Logs)
- Docker 容器日誌: `docker-compose logs -f [service_name]`
- 直接存取此目錄中的檔案

## 注意事項

此目錄被 git 忽略，避免大型日誌檔案被提交到版本庫中。