# Online Retail II ETL Pipeline

🚀 **一鍵部署的資料工程管線**，將英國線上零售商 2009-2011 年的交易資料轉換為可供分析的資料倉儲。

**完全獨立的 Docker 環境** - 無需現有 Airflow，包含完整的 ETL 流程、PostgreSQL 資料倉儲與品質監控系統。

## 🚀 快速開始

### 系統需求
- Docker & Docker Compose 已安裝
- 8GB+ 可用記憶體
- 5GB+ 可用磁碟空間

### 1️⃣ 準備資料檔案
下載 **online_retail_II.xlsx** 檔案（約 45MB）並放置到專案目錄：
```bash
# 建立資料目錄
mkdir -p data/raw

# 將 online_retail_II.xlsx 放入 data/raw/ 目錄
# 檔案來源：UCI Machine Learning Repository
```

### 2️⃣ 配置環境變數
複製環境變數範本並根據需要調整：

```bash
# 複製環境變數範本
cp .env.example .env

# 檢查配置檔案
ls -la .env*
```

**預設配置說明** (通常無需修改)：
- 資料庫連線：使用容器內 PostgreSQL (postgres-retail:5432)
- 資料來源路徑：`/opt/airflow/data/raw/online_retail_II.xlsx` (容器內路徑)
- 執行器：LocalExecutor (適合單機部署)
- 批次大小：10000 (適合一般硬體)

**需要修改的情況** (可選)：
```bash
# 編輯環境變數
nano .env
# 或
vim .env
```

常見調整項目：
- **電子郵件通知**：修改 `SMTP_*` 和 `ALERT_EMAIL` 設定
- **效能調整**：調整 `BATCH_SIZE` (10000-50000)
- **品質門檻**：調整 `DQ_THRESHOLD` (預設 0.95 = 95%)

### 3️⃣ 啟動服務

#### Unix/Linux/macOS
```bash
# 啟動所有服務
./start_standalone.sh

# 或者手動指令
docker-compose -f docker-compose-simple.yaml up -d
```

#### Windows
```cmd
REM 啟動所有服務
start_standalone.bat

REM 或者手動指令
docker-compose -f docker-compose-simple.yaml up -d
```

### 4️⃣ 驗證運行狀態
```bash
# 檢查服務狀態
docker-compose -f docker-compose-simple.yaml ps

# 預期看到 5 個服務運行中：
# - airflow-webserver (健康狀態)
# - airflow-scheduler (健康狀態)
# - airflow-init (完成狀態)
# - postgres (健康狀態)
# - postgres-retail (健康狀態)
```

### 5️⃣ 存取介面
服務啟動後可存取以下介面：

| 服務 | 網址 | 帳號/密碼 | 用途 |
|------|------|-----------|------|
| **Airflow Web UI** | http://localhost:8080 | airflow / airflow | ETL 管線監控與管理 |
| **PostgreSQL 資料倉儲** | localhost:5433 | airflow / airflow | 資料查詢與分析 |

### 6️⃣ 執行 ETL 管線
1. 開啟 Airflow Web UI (http://localhost:8080)
2. 找到 `retail_etl_pipeline` DAG
3. 點擊「啟用」開關（如果未啟用）
4. 點擊「Trigger DAG」手動執行
5. 監控執行進度，完整流程約需 5-10 分鐘

### 7️⃣ 查看結果
ETL 完成後：
- **Excel 品質報告**：`data/quality_reports/data_quality_report_YYYYMMDD_HHMM.xlsx`
- **資料倉儲**：透過 PostgreSQL 連線查詢星型結構表格
- **執行日誌**：Airflow UI 中查看詳細執行記錄

### 8️⃣ 停止服務

#### Unix/Linux/macOS
```bash
./stop_standalone.sh
```

#### Windows
```cmd
stop_standalone.bat
```

---

## 📋 專案概述

### 🎯 任務目標
將英國線上零售商其 `2009/12/01 – 2011/12/09` 的原始交易資料轉為可供分析的資料倉儲，並建立自動化的 ETL 流程，讓分析團隊可快速取得每日銷售、客戶分析與地區分佈等即時報表。

### 📊 資料集說明
- **來源**：online_retail_II.xlsx
- **時間範圍**：2009-12-01 至 2011-12-09
- **欄位**：InvoiceNo、StockCode、Quantity、UnitPrice、CustomerID、Country、InvoiceDate

### 🏗️ 技術架構
- **容器化**：Docker Compose 輕量化部署 (5個服務)
- **排程工具**：Apache Airflow 2.8.1 (LocalExecutor)
- **資料處理**：Python 3.8+, pandas, SQLAlchemy, openpyxl
- **資料庫**：雙 PostgreSQL 13 容器 (元數據 + 資料倉儲)
- **資料品質**：自訂 DQ 框架 + Excel 報表自動生成
- **監控**：Airflow Web UI + 容器日誌

## 🗂️ 專案結構

| 檔案/目錄 | 用途 | 說明 |
|----------|------|------|
| **🐳 容器配置** |
| `docker-compose-simple.yaml` | Docker 配置 | 5個服務，LocalExecutor |
| `Dockerfile` | 自訂 Airflow 映像 | 包含專案依賴套件 |
| `start_standalone.sh/.bat` | 啟動腳本 | 快速啟動服務 |
| `stop_standalone.sh/.bat` | 停止腳本 | 優雅停止服務 |
| **📋 核心程式** |
| `dags/` | Airflow DAG 定義 | ETL 管線編排邏輯 |
| `src/` | ETL 模組程式碼 | 資料處理核心功能 |
| `scripts/` | 獨立執行腳本 | 測試與工具程式 |
| **📊 資料與配置** |
| `data/raw/` | 原始資料目錄 | Excel 檔案存放 |
| `data/quality_reports/` | 品質報告目錄 | Excel 報告輸出 |
| `logs/` | 日誌目錄 | Airflow 執行日誌 |
| `.env.example` | 環境變數範本 | 配置檔案範例 |
| `.env` | 環境變數配置 | 資料庫連線、路徑設定 |
| `requirements.txt` | Python 依賴清單 | 套件安裝規格 |

## ⚙️ 環境變數設定

本專案使用 `.env` 檔案管理所有配置，您也可以透過 Airflow Variables 覆寫預設值。

### 核心設定 (.env)

```bash
# 資料庫配置 - Retail 資料倉儲
DB_HOST=postgres-retail
DB_PORT=5432
DB_NAME=retail_dw
DB_USER=airflow
DB_PASSWORD=airflow

# Airflow 配置
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# ETL 設定
DATA_SOURCE_PATH=/opt/airflow/data/raw/online_retail_II.xlsx
BATCH_SIZE=10000
LOG_LEVEL=INFO
```

### Airflow Variables (進階覆寫)
可在 Airflow UI → Admin → Variables 中設定：
- `retail_source_file`: 自訂資料來源路徑
- `postgres_retail_dw`: 自訂資料庫連線字串
- `etl_log_path`: 自訂日誌目錄

## 📊 Excel 品質監控報表

### 報表概述
系統會自動生成**單一 Excel 檔案**包含多個工作表，提供完整的資料品質分析。

**檔名格式**: `data_quality_report_YYYYMMDD_HHMM.xlsx`

### 工作表架構 (7個工作表)

| 工作表名稱 | 用途 | 內容說明 |
|-----------|------|----------|
| **Daily_Quality_Metrics** | 每日品質指標 | 每日銷售額、交易數、異常值偵測、品質評分 |
| **Customer_Data_Quality** | 客戶資料完整性 | 缺失客戶ID統計、完整性評分、品質狀態 |
| **Return_Rate_Analysis** | 退貨率分析 | 每日退貨率、異常偵測、趨勢分析 |
| **Product_Quality** | 產品資料品質 | 產品描述完整性、StockCode有效性、品質評分 |
| **Overall_Summary** | 整體摘要 | 總記錄數、營收統計、客戶產品數量、執行時間 |
| **Anomaly_Details** | 異常明細 | 銷售異常、退貨異常的詳細清單與嚴重程度 |
| **Monthly_Trends** | 月度趨勢 | 按月份統計的銷售趨勢、品質評分變化 |

### 特色功能
- ✅ **即用性**: 直接在 Excel 中開啟，無需額外工具
- 📈 **視覺化**: 內建圖表，條件格式高亮異常
- 🔄 **歷史追蹤**: 時間戳命名，支援趨勢分析
- 🎯 **BI 整合**: 可直接匯入 Tableau、PowerBI
- 📊 **多維分析**: 7個不同角度的品質分析維度

## 📊 ETL 流程說明

### 1. 資料擷取與 ETL 設計 ✅
- **Excel 讀取**：使用 Python（pandas、SQLAlchemy）、Airflow，自動化擷取原始 Excel 檔案
- **資料暫存**：將原始資料載入到 PostgreSQL staging table
- **基本轉換**：轉換資料型別、計算 `TotalAmount = Quantity * UnitPrice`

### 2. 資料清理與品質保證 ✅
- **空值檢查**：若 CustomerID、StockCode、InvoiceDate 為空則丟棄
- **負值與退貨處理**：退貨行（InvoiceNo 以 `C` 開頭或 Quantity < 0）標記為退貨
- **重複資料**：同筆 `InvoiceNo + StockCode + InvoiceDate` 重複行去重
- **異常偵測**：`UnitPrice ≤ 0` 或 `Quantity > 1000`，標記並輸出警示

### 3. 資料倉儲模型設計 (Star Schema) ✅

#### 🎯 事實表
**fact_sales**
- sale_id (PK)
- invoice_no
- product_key (FK → dim_product)
- customer_key (FK → dim_customer)
- time_key (FK → dim_time)
- country_key (FK → dim_country)
- quantity
- unit_price
- total_amount
- is_return

#### 📏 維度表

**dim_product**
- product_key (PK)
- stock_code
- description

**dim_customer**
- customer_key (PK)
- customer_id
- country_key (FK)

**dim_time**
- time_key (PK)
- date_value
- year, month, quarter
- day_of_week, day_name
- is_weekend

**dim_country**
- country_key (PK)
- country_name

### 4. 自動化與監控機制 ✅
- **ETL 排程**：使用 Airflow 每日自動化執行 (凌晨 2 點)
- **資料品質檢查**：完整的 DQ 檢查框架，檢查每日銷售總額範圍、缺失客戶 ID 比例
- **警示機制**：失敗時自動發送電子郵件通知
- **監控報表**：生成 Excel 格式的資料品質報告

## 🏗️ 詳細專案結構

```
retail_etl_standalone/
├── 🐳 容器配置
│   ├── docker-compose-simple.yaml # Docker 配置檔案
│   ├── Dockerfile                 # 自訂 Airflow 映像
│   └── requirements.txt           # Python 相依套件
├── 🚀 啟動腳本
│   ├── start_standalone.sh/.bat   # 服務啟動腳本
│   └── stop_standalone.sh/.bat    # 服務停止腳本
├── 📋 DAG 定義
│   └── dags/
│       └── retail_etl_dag.py      # 主要 ETL 管線
├── 💻 ETL 核心程式
│   └── src/
│       ├── etl/                   # ETL 處理模組
│       │   ├── extract/           # 資料擷取
│       │   ├── transform/         # 資料轉換
│       │   ├── load/              # 資料載入
│       │   ├── data_quality.py    # 品質檢查
│       │   └── quality_monitoring.py # 品質監控
│       ├── configs/               # YAML 配置檔案
│       ├── sql/                   # SQL 腳本 (create_tables.sql, dq_checks.sql)
│       └── utils/                 # 工具函數
├── 🧪 測試與工具
│   └── scripts/
│       └── export_quality_data.py # 獨立品質監控腳本
├── 📊 資料存儲
│   └── data/
│       ├── raw/                   # 原始 Excel 檔案
│       └── quality_reports/       # Excel 品質報告輸出
├── 📝 日誌系統
│   └── logs/                      # Airflow 執行日誌
└── ⚙️ 配置檔案
    ├── .env.example               # 環境變數範本
    └── .env                       # 環境變數配置
```

## ⚙️ Airflow DAG 設計

### 主要 ETL Pipeline (retail_etl_pipeline)

```
start_pipeline → create_tables → extract_data → transform_data → validate_quality
                                                                      ↓
cleanup_staging ← notify_success ← run_quality_monitoring ← run_dq_checks
                                                                      ↓
                                                                 end_pipeline
```

### 任務說明
1. **create_tables**: 建立資料庫表格結構
2. **extract_data**: 從 Excel 擷取資料到 staging table
3. **transform_data**: 轉換為 Star Schema 維度模型
4. **validate_quality**: 執行資料品質驗證
5. **run_dq_checks**: 執行 SQL 資料品質檢查
6. **run_quality_monitoring**: 生成品質監控報表
7. **notify_success**: 發送成功通知郵件
8. **cleanup_staging**: 清理舊的 staging 資料

## ⚙️ 系統配置

### 容器服務對應
| 服務 | 內部端口 | 外部端口 | 用途 |
|------|----------|----------|------|
| airflow-webserver | 8080 | 8080 | Web UI |
| postgres-retail | 5432 | 5433 | 資料倉儲 |
| postgres | 5432 | 5432 | Airflow 元數據 |
| airflow-scheduler | - | - | 任務調度 |

## 🧪 測試與驗證

### 手動測試 DAG

```bash
# 測試 DAG 語法
docker exec retail_etl_standalone-airflow-webserver-1 \
  python /opt/airflow/dags/retail_etl_dag.py

# 測試特定任務
docker exec retail_etl_standalone-airflow-webserver-1 \
  airflow tasks test retail_etl_pipeline extract_data 2024-01-01

# 測試完整 DAG
docker exec retail_etl_standalone-airflow-webserver-1 \
  airflow dags test retail_etl_pipeline 2024-01-01
```

### 資料品質驗證

```bash
# 檢查資料表內容
psql -h localhost -p 5433 -U airflow -d retail_dw -c "
SELECT
  'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales
UNION ALL
SELECT
  'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT
  'dim_product', COUNT(*) FROM dim_product;
"

# 檢查品質報告
ls -la data/quality_reports/
```

### 獨立測試腳本

```bash
# 測試完整品質監控流程（無需 Airflow）
python scripts/export_quality_data.py

# 測試資料庫連線
python -c "from src.etl.extract.retail_extract import RetailExtractor; RetailExtractor().test_connection()"
```

## 📊 監控與警示

### 資料品質指標
- **品質分數**: 通過檢查項目的百分比 (目標 ≥ 95%)
- **處理統計**: 處理行數、丟棄率、重複率
- **業務指標**: 總營收、退貨率、客戶數、國家覆蓋數

### 自動化報表
- **Excel 品質報告**: 每次執行後自動生成
- **電子郵件通知**: 成功/失敗都會發送詳細報告
- **Airflow UI 監控**: 即時查看任務執行狀態

### 日誌監控
- **結構化日誌**: 輸出到 Docker logs 和 Airflow UI
- **錯誤追蹤**: 完整的錯誤堆疊資訊
- **效能指標**: 處理時間、記憶體使用量

## 🎨 架構特色

### 🐳 容器化優勢
- **環境一致性**: Docker 確保開發/測試/生產環境完全相同
- **快速部署**: 一鍵啟動完整的 Airflow + PostgreSQL 環境
- **資源隔離**: 服務間獨立運作，降低相互影響
- **版本控制**: 容器映像版本化，可回滾到任意穩定版本

### 🔧 配置管理策略
```
優先級 (高 → 低):
Airflow Variables → 環境變數 → .env 檔案
```

**設計優點**:
- 🛠️ **開發友善**: .env 檔案提供預設配置
- 🚀 **部署彈性**: 環境變數適用不同部署環境
- ⚡ **即時調整**: Airflow Variables 支援熱更新

### 📂 路徑映射設計

```
本機端路徑 ←→ 容器內路徑
./data/     ←→ /opt/airflow/data/
./logs/     ←→ /opt/airflow/logs/
./dags/     ←→ /opt/airflow/dags/
./src/      ←→ /opt/airflow/src/
```

#### 🗂️ 資料流向
```
原始 Excel → 容器內處理 → PostgreSQL → 品質報告
     ↓              ↓            ↓           ↓
主機端掛載      容器內路徑      持久化儲存    主機端輸出
./data/raw  → /opt/airflow/  → postgres-  → ./data/quality_
                data/          retail       reports/
```

## 🎉 交付項目

### ✅ 已完成項目

1. **ETL 腳本**
   - ✅ Airflow DAG 完整實作 (retail_etl_dag.py, 361行)
   - ✅ Python ETL 模組化設計 (extract/transform/load)
   - ✅ 資料品質檢查與監控機制

2. **資料倉儲建表與載入腳本**
   - ✅ SQL 建表腳本 (create_tables.sql)
   - ✅ Star Schema 實作
   - ✅ 自動化資料載入與驗證

3. **品質檢查與警示程式**
   - ✅ 多層次資料品質檢查
   - ✅ Excel 報表自動生成
   - ✅ 電子郵件通知機制

4. **README.md 說明文件**
   - ✅ 跨平台啟動腳本 (sh/bat)
   - ✅ 一鍵部署與啟動流程
   - ✅ 容器化環境設定指南
   - ✅ 完整測試與驗證步驟

5. **BI 儀表板支援**
   - ✅ Star Schema 設計支援 OLAP 查詢
   - ✅ 品質監控報表 (Excel 格式)
   - ✅ 關鍵業務指標追蹤