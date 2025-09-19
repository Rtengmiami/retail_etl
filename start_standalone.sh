#!/bin/bash

# Online Retail II ETL Pipeline - Standalone Deployment
# 獨立版本啟動腳本

echo "啟動 Online Retail II ETL Pipeline (獨立版本)..."

# 設定必要的環境變數
export AIRFLOW_UID=$(id -u)
export AIRFLOW_PROJ_DIR=$(pwd)
export PROJECT_ROOT=$(pwd)

echo ""
echo "環境設定:"
echo "   AIRFLOW_UID: ${AIRFLOW_UID}"
echo "   AIRFLOW_PROJ_DIR: ${AIRFLOW_PROJ_DIR}"
echo "   PROJECT_ROOT: ${PROJECT_ROOT}"
echo "   當前目錄: $(pwd)"

# 建立必要目錄
mkdir -p logs data/raw data/processed data/quality_reports

echo ""
echo "檢查關鍵檔案..."
if [ -f "docker-compose.yaml" ]; then
    echo "   ✅ Docker Compose 檔案存在"
else
    echo "   ❌ Docker Compose 檔案不存在"
    exit 1
fi

if [ -f ".env" ]; then
    echo "   ✅ 環境變數檔案存在"
else
    echo "   ❌ 環境變數檔案不存在"
    exit 1
fi

if [ -f "data/raw/online_retail_II.xlsx" ]; then
    echo "   ✅ 數據文件存在"
else
    echo "   ⚠️  數據文件不存在: data/raw/online_retail_II.xlsx"
    echo "      請確認已下載 online_retail_II.xlsx 到 data/raw/ 目錄"
fi

echo ""
echo "啟動 Airflow 服務..."

# 啟動服務
docker-compose -f docker-compose-simple.yaml up -d

echo ""
echo "等待服務啟動..."
sleep 10

# 檢查服務狀態
echo "服務狀態:"
docker-compose -f docker-compose-simple.yaml ps

echo ""
echo "啟動完成！"
echo ""
echo "Airflow UI: http://localhost:8080"
echo "預設帳號: airflow / airflow"
echo ""
echo "資料庫連線:"
echo "   PostgreSQL (Retail DW): localhost:5433"
echo "   - 資料庫: retail_dw"
echo "   - 用戶: airflow / airflow"
echo ""
echo "查看日誌："
echo "   docker-compose logs -f [service_name]"
echo ""
echo "停止服務："
echo "   docker-compose down"